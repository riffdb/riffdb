#![expect(
    clippy::expect_used,
    reason = "validated symbolic plans retain their schema references, result bounds, and closed aggregate shapes"
)]

//! Symbolic RiffQL application operations over the shared service boundary.

use std::collections::{BTreeMap, BTreeSet};
use std::num::NonZeroU16;
use std::sync::Arc;
use std::time::{Duration, Instant};

use riffdb_application::{
    ApplicationPortabilityValidation, ReimportObservation, ReimportObservationResult,
};
pub use riffdb_catalog::{
    APPLICATION_CATALOG_SCHEMA_V1, ApplicationCatalogFeatureStateV1, ApplicationCatalogFeatureV1,
    ApplicationCatalogFeatureViewV1, ApplicationCatalogPageV1, ApplicationCatalogSourceSpanV1,
    ApplicationCatalogSymbolKindV1, ApplicationCatalogSymbolV1, MAX_APPLICATION_CATALOG_PAGE_ITEMS,
};
use riffdb_catalog::{
    ActiveQueryModuleExpectation, ApplicationCatalogAuthorityV1, ApplicationCatalogCandidatesV1,
    PreparedQueryModuleActivation, PreparedReactiveModulePublication, ValidatedQueryModule,
    ValidatedReactiveModule,
};
use riffdb_columnar::ColumnPredicate;
use riffdb_commit::{
    ControlPlaneExecutionErrorKind, ControlPlaneTerminalAudit,
    QueryModuleDeploymentOutcome as CoordinatorQueryModuleDeploymentOutcome,
    QueryModuleDeploymentPreparation,
    ReactiveModulePublicationOutcome as CoordinatorReactiveModulePublicationOutcome,
    ReactiveModulePublicationPreparation,
};
use riffdb_contract_ir::{ValueType, ValueTypeTag};
use riffdb_errors::{
    ApplicationErrorCode, PublicError, ValidationCode, ValidationIssue, ValidationIssues,
    ValidationPath,
};
use riffdb_policy::{
    ApplicationCatalogQueryCandidate, ApplicationQueryAccessRequirement, ApplicationQueryTarget,
    AuthorizedApplicationQuery, AuthorizedApplicationReimportV1, AuthorizedQueryRowPolicyContextV1,
    CommandToolCandidate, DiscoveryVisibility, MAX_DISCOVERY_PAGE_ITEMS, NamedQueryToolCandidate,
    OperationRequest, OperationTenantScope, OutputClassification, PartitionConstraint,
    resolve_authorized_query_row_policy_context,
};
use riffdb_projection::{
    ProviderEpochObservationV1, ResultSetEpochContextV1, ResultSetEpochRequirementV1,
    negotiate_result_set_epoch_v1,
};
use riffdb_query_compiler::{PlannerDiagnostic, compile_query};
pub use riffdb_query_executor::CoveredQueryResultV1;
use riffdb_query_executor::{
    LongPatternCandidateBatch, QueryAggregateCell, QueryAggregateRow, QueryContinuation,
    QueryExecutionError, QueryOwnedSnapshot, QueryParameters, QueryResultValue, QueryRow,
};
use riffdb_query_ir::{
    ExactParameterValueV1, ExactScalarV1, NamedTypeSchema, ProjectedVectorFreshnessV1,
    QueryAccessKind, QueryAccessProgramV1, QueryDiagnostic, QueryPredicateOperator,
    QueryPredicateValue, SymbolicCatalog, resolve_query_surface,
};
use riffdb_query_module::{
    CompiledExactPredicateResultSetV1, CompiledNamedQuery,
    CompiledNullableExactPredicateResultSetV1, NamedQuerySource, QueryModuleCandidate,
};
use riffdb_riffql_syntax::{
    Document, Literal, MAX_IDENTIFIER_BYTES, ParseDiagnostic, Span, TypeReference, parse_query,
};
use riffdb_types::{
    CanonicalValue, CommitSequence, CompiledLongPatternV1, ContractBundleHash, ContractLineage,
    ContractVersion, ExactTextProfileV1, FieldId, FrontierPosition, QueryModuleHash,
    QueryModuleName, QueryModuleVersion, QueryOperationName, QueryPlanHash, ReactiveModuleHash,
    ServiceAuditLinkV1, ServiceAuditPhaseV1, ServiceOperationV1, encode_canonical_value,
    hash_generated_artifact, hash_query_parameters,
};

use crate::command_operations::{SubmittedValueMaterializationError, materialize_submitted_value};
use crate::orchestration::AuditScope;
use crate::query_discovery_operations::{
    finish_discovery_result, finish_failure, finish_success, prepare_selected_contract,
    read_active_query_module_for_discovery,
};
use crate::wait::{ControlledWaitError, wait_with_control};
use crate::{
    ApplicationCatalogCursorLookup, ApplicationCatalogCursorState, AuthoritativeReadError,
    ContractSelection, CursorAccessError, CursorContractIdentity, CursorToken,
    ExactPredicateProjectionRequest, ExactPredicateProjectionResult, ExactTextProjectionPortError,
    ExactTextProjectionRequest, ExactTextProjectionResult, InternalDefect,
    LongPatternProjectionRequest, NullableExactPredicateProjectionRequest, PortAdmissionError,
    PortDriverStopped, QueryCursorLookup, QueryCursorState, ReadPipelineStage, RequestContext,
    RiffDbService, RiffDbServiceInner, ServiceAuditTargetMap, ServiceFailure, ServiceFuture,
    ServiceResult, ServiceTelemetryEvent, SourceName, SubmittedEnum, SubmittedValue,
    TokenizedTextCursorState, TokenizedTextProjectionRequest, VectorProjectionPortError,
    VectorProjectionRequest,
};

async fn resolve_admission_head_floor(
    service: &RiffDbServiceInner,
    context: &RequestContext,
    explicit: Option<u64>,
    consistency: Option<QueryConsistencyV1>,
) -> Result<Option<u64>, ServiceFailure> {
    if consistency.is_none() {
        return Ok(explicit);
    }
    let permit = match wait_with_control(
        context.control(),
        service.providers.deadline_scheduler.as_ref(),
        service
            .providers
            .authoritative
            .reserve_application_head(context.control()),
    )
    .await
    {
        Ok(Ok(permit)) => permit,
        Ok(Err(PortAdmissionError::Cancelled)) => return Err(ServiceFailure::Cancelled),
        Ok(Err(PortAdmissionError::DeadlineExceeded)) => {
            return Err(ServiceFailure::DeadlineExceeded);
        }
        Ok(Err(PortAdmissionError::Unavailable | PortAdmissionError::Stopped)) => {
            return Err(PublicError::storage_unavailable().into());
        }
        Err(ControlledWaitError::Cancelled) => return Err(ServiceFailure::Cancelled),
        Err(ControlledWaitError::DeadlineExceeded) => {
            return Err(ServiceFailure::DeadlineExceeded);
        }
    };
    let receipt = permit.submit(()).map_err(|error| match error {
        PortAdmissionError::Cancelled => ServiceFailure::Cancelled,
        PortAdmissionError::DeadlineExceeded => ServiceFailure::DeadlineExceeded,
        PortAdmissionError::Unavailable | PortAdmissionError::Stopped => {
            PublicError::storage_unavailable().into()
        }
    })?;
    let head = match wait_with_control(
        context.control(),
        service.providers.deadline_scheduler.as_ref(),
        receipt,
    )
    .await
    {
        Ok(Ok(Ok(head))) => head,
        Ok(Ok(Err(AuthoritativeReadError::Cancelled))) => return Err(ServiceFailure::Cancelled),
        Ok(Ok(Err(AuthoritativeReadError::DeadlineExceeded))) => {
            return Err(ServiceFailure::DeadlineExceeded);
        }
        Ok(Ok(Err(AuthoritativeReadError::Unavailable))) => {
            return Err(PublicError::storage_unavailable().into());
        }
        Ok(Ok(Err(
            AuthoritativeReadError::HistoryPruned
            | AuthoritativeReadError::Integrity
            | AuthoritativeReadError::InvalidContinuation,
        )))
        | Ok(Err(PortDriverStopped)) => {
            return Err(service.internal_failure(
                ServiceOperationV1::ExecuteQuery,
                InternalDefect::ProofMismatch,
            ));
        }
        Err(ControlledWaitError::Cancelled) => return Err(ServiceFailure::Cancelled),
        Err(ControlledWaitError::DeadlineExceeded) => {
            return Err(ServiceFailure::DeadlineExceeded);
        }
    };
    let captured = match head {
        FrontierPosition::BeforeFirst => None,
        FrontierPosition::AppliedThrough(sequence) => Some(sequence.get()),
    };
    Ok(match (explicit, captured) {
        (Some(left), Some(right)) => Some(left.max(right)),
        (Some(value), None) | (None, Some(value)) => Some(value),
        (None, None) => None,
    })
}

/// Stricter application-surface source ceiling.
pub const MAX_SYMBOLIC_QUERY_SOURCE_BYTES: usize = 262_144;
/// Maximum access steps admitted by one interactive application request.
pub const MAX_SYMBOLIC_QUERY_STEPS: usize = 64;
/// Maximum reactive source bytes admitted by the application service.
pub const MAX_REACTIVE_MODULE_SOURCE_INPUT_BYTES: usize = 1_048_576;
/// Maximum exact query modules admitted for one reactive compilation.
pub const MAX_REACTIVE_QUERY_MODULE_INPUTS: usize = 32;

/// One stronger server-resolved query consistency request.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum QueryConsistencyV1 {
    /// Capture the authoritative application head after admission and require
    /// the first-page snapshot/provider epoch to reach it.
    AdmissionHead,
}

/// Maximum time one exact-result request waits for its background-owned provider slot.
const EXACT_PROVIDER_READINESS_WAIT: Duration = Duration::from_millis(250);
/// Poll cadence for observing publication without blocking an async service worker.
const EXACT_PROVIDER_READINESS_POLL: Duration = Duration::from_millis(10);
/// Hard observation ceiling, including the initial provider attempt.
const MAX_EXACT_PROVIDER_READINESS_OBSERVATIONS: u16 = 32;

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

    pub(crate) fn selection(&self) -> &ContractSelection {
        &self.selection
    }

    /// Returns the complete exact identity when the selector is hash-pinned.
    #[must_use]
    pub fn exact_identity(
        &self,
    ) -> Option<(&ContractLineage, ContractVersion, ContractBundleHash)> {
        match (&self.selection, self.expected_hash) {
            (ContractSelection::Exact { lineage, version }, Some(hash)) => {
                Some((lineage, *version, hash))
            }
            _ => None,
        }
    }

    pub(crate) fn matches(&self, bundle: &riffdb_catalog::ValidatedContractBundle) -> bool {
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

    /// Test-only identity for golden conversion fixtures.
    #[cfg(any(test, feature = "test-fixtures"))]
    #[doc(hidden)]
    pub fn from_parts_for_test(
        lineage: ContractLineage,
        version: ContractVersion,
        bundle_hash: ContractBundleHash,
        name: Option<String>,
        plan_hash: QueryPlanHash,
    ) -> Self {
        Self {
            lineage,
            version,
            bundle_hash,
            name,
            plan_hash,
            module_hash: None,
        }
    }

    fn from_named(program: &QueryAccessProgramV1, module_hash: QueryModuleHash) -> Self {
        let mut identity = Self::from_program(program);
        identity.module_hash = Some(module_hash);
        identity
    }

    fn from_named_plan(
        program: &QueryAccessProgramV1,
        module_hash: QueryModuleHash,
        plan_hash: QueryPlanHash,
    ) -> Self {
        let mut identity = Self::from_named(program, module_hash);
        identity.plan_hash = plan_hash;
        identity
    }

    fn from_named_query(query: &CompiledNamedQuery, module_hash: QueryModuleHash) -> Self {
        let program = query.plan().representative_program();
        Self {
            lineage: program.contract().lineage().clone(),
            version: program.contract().version(),
            bundle_hash: program.contract().bundle_hash(),
            name: Some(query.name().to_owned()),
            plan_hash: query.plan().identity(),
            module_hash: Some(module_hash),
        }
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
        Self::from_schemas(program.surface().schemas())
    }

    fn from_named_query(query: &CompiledNamedQuery) -> Self {
        Self::from_schemas(query.plan().schemas())
    }

    fn from_schemas(schemas: &riffdb_query_ir::NamedQuerySchemas) -> Self {
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

    fn from_named_query(query: &CompiledNamedQuery, module_hash: QueryModuleHash) -> Self {
        Self {
            identity: SymbolicQueryIdentity::from_named_query(query, module_hash),
            schema: SymbolicQuerySchema::from_named_query(query),
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

/// One bounded authorization-filtered application-catalog request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ApplicationCatalogRequest {
    contract: SymbolicContractSelector,
    limit: NonZeroU16,
    cursor: Option<CursorToken>,
}

impl ApplicationCatalogRequest {
    /// Checks the public catalog page ceiling.
    pub fn new(
        contract: SymbolicContractSelector,
        limit: NonZeroU16,
        cursor: Option<CursorToken>,
    ) -> Result<Self, SymbolicQueryInputError> {
        if usize::from(limit.get()) > MAX_APPLICATION_CATALOG_PAGE_ITEMS {
            return Err(SymbolicQueryInputError::TooLong);
        }
        Ok(Self {
            contract,
            limit,
            cursor,
        })
    }

    /// Exact selected contract.
    #[must_use]
    pub const fn contract(&self) -> &SymbolicContractSelector {
        &self.contract
    }

    /// Requested bounded visible-symbol count.
    #[must_use]
    pub const fn limit(&self) -> NonZeroU16 {
        self.limit
    }

    /// Opaque continuation, when this is not the first page.
    #[must_use]
    pub const fn cursor(&self) -> Option<CursorToken> {
        self.cursor
    }
}

/// One bounded symbolic application-catalog page and opaque continuation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ApplicationCatalogResult {
    page: ApplicationCatalogPageV1,
    next_cursor: Option<CursorToken>,
}

impl ApplicationCatalogResult {
    /// Authorization-filtered name-only page.
    #[must_use]
    pub const fn page(&self) -> &ApplicationCatalogPageV1 {
        &self.page
    }

    /// Opaque next-page token, present exactly when the page has more symbols.
    #[must_use]
    pub const fn next_cursor(&self) -> Option<CursorToken> {
        self.next_cursor
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

/// Checked immutable reactive-module publication request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeployReactiveModuleRequest {
    contract: SymbolicContractSelector,
    source: String,
    query_module_hashes: Vec<QueryModuleHash>,
}

impl DeployReactiveModuleRequest {
    /// Validates the bounded source and canonical exact dependency inventory.
    pub fn new(
        contract: SymbolicContractSelector,
        source: String,
        query_module_hashes: Vec<QueryModuleHash>,
    ) -> Result<Self, SymbolicQueryInputError> {
        if source.is_empty()
            || source.len() > MAX_REACTIVE_MODULE_SOURCE_INPUT_BYTES
            || query_module_hashes.len() > MAX_REACTIVE_QUERY_MODULE_INPUTS
            || query_module_hashes
                .windows(2)
                .any(|pair| pair[0] >= pair[1])
        {
            return Err(SymbolicQueryInputError::TooLong);
        }
        Ok(Self {
            contract,
            source,
            query_module_hashes,
        })
    }
}

/// Name-only immutable reactive-module descriptor.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReactiveModuleDescriptor {
    name: String,
    version: u64,
    hash: ReactiveModuleHash,
    contract_lineage: ContractLineage,
    contract_version: ContractVersion,
    contract_hash: ContractBundleHash,
    query_module_hashes: Vec<QueryModuleHash>,
    operation_names: Vec<String>,
}

impl ReactiveModuleDescriptor {
    fn from_module(module: &ValidatedReactiveModule) -> Self {
        Self {
            name: module.plan().name().to_owned(),
            version: module.plan().version(),
            hash: module.identity(),
            contract_lineage: module.plan().contract_lineage().clone(),
            contract_version: module.plan().contract_version(),
            contract_hash: module.plan().contract_hash(),
            query_module_hashes: riffdb_query_module::reactive_module_query_dependencies(
                module.plan(),
            ),
            operation_names: module
                .plan()
                .operations()
                .iter()
                .map(|operation| operation.name().as_str().to_owned())
                .collect(),
        }
    }

    /// Symbolic module name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Positive source-declared version.
    #[must_use]
    pub const fn version(&self) -> u64 {
        self.version
    }

    /// Immutable module identity.
    #[must_use]
    pub const fn hash(&self) -> ReactiveModuleHash {
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

    /// Exact query-module dependencies in canonical hash order.
    #[must_use]
    pub fn query_module_hashes(&self) -> &[QueryModuleHash] {
        &self.query_module_hashes
    }

    /// Reactive operation names in canonical compiler order.
    #[must_use]
    pub fn operation_names(&self) -> &[String] {
        &self.operation_names
    }
}

/// Closed immutable reactive-module publication disposition.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ReactiveModuleDeploymentDisposition {
    /// A new immutable module was published.
    Published,
    /// The exact immutable module was already retained.
    AlreadyPublished,
    /// The same symbolic name and version are retained with different content.
    ModuleVersionConflict,
    /// The exact contract ceased to be retained before commit.
    ContractUnavailable,
    /// One exact query-module dependency ceased to be retained before commit.
    QueryModuleUnavailable(QueryModuleHash),
}

/// Safe reactive-module publication response.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeployReactiveModuleResult {
    outcome: ReactiveModuleDeploymentDisposition,
    module: ReactiveModuleDescriptor,
}

impl DeployReactiveModuleResult {
    /// Closed publication outcome.
    #[must_use]
    pub const fn outcome(&self) -> &ReactiveModuleDeploymentDisposition {
        &self.outcome
    }

    /// Submitted immutable module identity.
    #[must_use]
    pub const fn module(&self) -> &ReactiveModuleDescriptor {
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
    consistency: Option<QueryConsistencyV1>,
    accepts_compact_result_v1: bool,
    accepts_packed_result_v1: bool,
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
            consistency: None,
            accepts_compact_result_v1: false,
            accepts_packed_result_v1: false,
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

    /// Requires the first-page snapshot/provider epoch to reach the
    /// authoritative application head observed after admission.
    #[must_use]
    pub const fn with_admission_head_consistency(mut self) -> Self {
        self.consistency = Some(QueryConsistencyV1::AdmissionHead);
        self
    }

    /// Advertises support for the additive compact named-result encoding.
    #[must_use]
    pub const fn accepting_compact_result_v1(mut self) -> Self {
        self.accepts_compact_result_v1 = true;
        self
    }

    /// Advertises support for the additive packed named-result encoding.
    #[must_use]
    pub const fn accepting_packed_result_v1(mut self) -> Self {
        self.accepts_compact_result_v1 = true;
        self.accepts_packed_result_v1 = true;
        self
    }

    /// Minimum authoritative application head required by this read.
    #[must_use]
    pub const fn minimum_application_head(&self) -> Option<u64> {
        self.minimum_application_head
    }

    /// Stronger server-resolved consistency class, when requested.
    #[must_use]
    pub const fn consistency(&self) -> Option<QueryConsistencyV1> {
        self.consistency
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
    consistency: Option<QueryConsistencyV1>,
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
            consistency: None,
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

    /// Requires the first-page snapshot/provider epoch to reach the
    /// authoritative application head observed after admission.
    #[must_use]
    pub const fn with_admission_head_consistency(mut self) -> Self {
        self.consistency = Some(QueryConsistencyV1::AdmissionHead);
        self
    }

    /// Minimum authoritative application head required by this read.
    #[must_use]
    pub const fn minimum_application_head(&self) -> Option<u64> {
        self.minimum_application_head
    }

    /// Stronger server-resolved consistency class, when requested.
    #[must_use]
    pub const fn consistency(&self) -> Option<QueryConsistencyV1> {
        self.consistency
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
///
/// Entity and field **names** are shared `Arc<str>` handles from the query plan;
/// field **values** are moved from the executor snapshot (no third clone).
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SymbolicResultRecord {
    entity: Arc<str>,
    fields: BTreeMap<Arc<str>, CanonicalValue>,
    nested: BTreeMap<Arc<str>, Vec<SymbolicResultRecord>>,
    exact_decimals: BTreeMap<Arc<str>, ExactDecimalResult>,
    exact_means: BTreeMap<Arc<str>, ExactMeanResult>,
}

/// Move-only components of one symbolic result record.
pub type SymbolicResultRecordParts = (
    Arc<str>,
    BTreeMap<Arc<str>, CanonicalValue>,
    BTreeMap<Arc<str>, Vec<SymbolicResultRecord>>,
    BTreeMap<Arc<str>, ExactDecimalResult>,
    BTreeMap<Arc<str>, ExactMeanResult>,
);

/// Full-width exact decimal returned by an operational aggregate.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ExactDecimalResult {
    coefficient: i128,
    scale: u8,
}

impl ExactDecimalResult {
    /// Signed fixed-scale coefficient.
    #[must_use]
    pub const fn coefficient(self) -> i128 {
        self.coefficient
    }

    /// Declared scale.
    #[must_use]
    pub const fn scale(self) -> u8 {
        self.scale
    }
}

/// Full-width exact mean state returned without division or rounding.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ExactMeanResult {
    coefficient: i128,
    scale: u8,
    count: u64,
}

impl ExactMeanResult {
    /// Signed total coefficient at the declared scale.
    #[must_use]
    pub const fn coefficient(self) -> i128 {
        self.coefficient
    }

    /// Declared scale of the exact total.
    #[must_use]
    pub const fn scale(self) -> u8 {
        self.scale
    }

    /// Exact number of contributing rows.
    #[must_use]
    pub const fn count(self) -> u64 {
        self.count
    }
}

impl SymbolicResultRecord {
    fn from_row(row: QueryRow) -> Self {
        let (entity, fields, nested) = row.into_parts();
        Self {
            entity,
            fields,
            nested: nested
                .into_iter()
                .map(|(name, rows)| (name, rows.into_iter().map(Self::from_row).collect()))
                .collect(),
            exact_decimals: BTreeMap::new(),
            exact_means: BTreeMap::new(),
        }
    }

    fn from_aggregate(row: QueryAggregateRow) -> Self {
        let (entity, cells) = row.into_parts();
        let mut fields = BTreeMap::new();
        let mut exact_decimals = BTreeMap::new();
        let mut exact_means = BTreeMap::new();
        for (name, cell) in cells {
            match cell {
                QueryAggregateCell::Canonical(value) => {
                    fields.insert(name, value);
                }
                QueryAggregateCell::ExactDecimal { coefficient, scale } => {
                    exact_decimals.insert(name, ExactDecimalResult { coefficient, scale });
                }
                QueryAggregateCell::ExactMean {
                    coefficient,
                    scale,
                    count,
                } => {
                    exact_means.insert(
                        name,
                        ExactMeanResult {
                            coefficient,
                            scale,
                            count,
                        },
                    );
                }
            }
        }
        Self {
            entity,
            fields,
            nested: BTreeMap::new(),
            exact_decimals,
            exact_means,
        }
    }

    /// Test-only constructor for golden conversion fixtures.
    #[cfg(any(test, feature = "test-fixtures"))]
    #[doc(hidden)]
    pub fn from_shared_for_test(
        entity: Arc<str>,
        fields: BTreeMap<Arc<str>, CanonicalValue>,
    ) -> Self {
        Self {
            entity,
            fields,
            nested: BTreeMap::new(),
            exact_decimals: BTreeMap::new(),
            exact_means: BTreeMap::new(),
        }
    }

    /// Test-only constructor for nested expansion carriage fixtures.
    #[cfg(any(test, feature = "test-fixtures"))]
    #[doc(hidden)]
    pub fn from_nested_for_test(
        entity: Arc<str>,
        fields: BTreeMap<Arc<str>, CanonicalValue>,
        nested: BTreeMap<Arc<str>, Vec<Self>>,
    ) -> Self {
        Self {
            entity,
            fields,
            nested,
            exact_decimals: BTreeMap::new(),
            exact_means: BTreeMap::new(),
        }
    }

    /// Test-only aggregate constructor for transport carriage fixtures.
    #[cfg(any(test, feature = "test-fixtures"))]
    #[doc(hidden)]
    pub fn from_aggregate_for_test(
        entity: Arc<str>,
        fields: BTreeMap<Arc<str>, CanonicalValue>,
        exact_decimals: BTreeMap<Arc<str>, (i128, u8)>,
    ) -> Self {
        Self {
            entity,
            fields,
            nested: BTreeMap::new(),
            exact_decimals: exact_decimals
                .into_iter()
                .map(|(name, (coefficient, scale))| {
                    (name, ExactDecimalResult { coefficient, scale })
                })
                .collect(),
            exact_means: BTreeMap::new(),
        }
    }

    /// Test-only exact-mean constructor for transport carriage fixtures.
    #[cfg(any(test, feature = "test-fixtures"))]
    #[doc(hidden)]
    pub fn from_exact_mean_for_test(
        entity: Arc<str>,
        name: Arc<str>,
        coefficient: i128,
        scale: u8,
        count: u64,
    ) -> Self {
        Self {
            entity,
            fields: BTreeMap::new(),
            nested: BTreeMap::new(),
            exact_decimals: BTreeMap::new(),
            exact_means: BTreeMap::from([(
                name,
                ExactMeanResult {
                    coefficient,
                    scale,
                    count,
                },
            )]),
        }
    }

    /// Contract entity name.
    #[must_use]
    pub fn entity(&self) -> &str {
        &self.entity
    }

    /// Fields in canonical name order (shared name handles).
    #[must_use]
    pub const fn fields(&self) -> &BTreeMap<Arc<str>, CanonicalValue> {
        &self.fields
    }

    /// Nested expansion fields in canonical name and driver order.
    #[must_use]
    pub const fn nested(&self) -> &BTreeMap<Arc<str>, Vec<SymbolicResultRecord>> {
        &self.nested
    }

    /// Full-width decimal fields in canonical name order.
    #[must_use]
    pub const fn exact_decimals(&self) -> &BTreeMap<Arc<str>, ExactDecimalResult> {
        &self.exact_decimals
    }

    /// Full-width exact mean fields in canonical name order.
    #[must_use]
    pub const fn exact_means(&self) -> &BTreeMap<Arc<str>, ExactMeanResult> {
        &self.exact_means
    }

    /// Consumes the record into owned name handles and values.
    #[must_use]
    pub fn into_parts(self) -> SymbolicResultRecordParts {
        (
            self.entity,
            self.fields,
            self.nested,
            self.exact_decimals,
            self.exact_means,
        )
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

/// Shared enum display-name table for one published contract schema.
///
/// Alias of the catalog publication table so response assembly and transport
/// share one Arc allocation for the lifetime of the validated bundle.
pub type SharedEnumVariantNames = riffdb_catalog::ContractEnumVariantNames;

/// Complete one-snapshot execution result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExecuteSymbolicQueryResult {
    identity: SymbolicQueryIdentity,
    outcome: String,
    application_head: u64,
    fields: BTreeMap<String, SymbolicResultField>,
    compact_result: Option<CoveredQueryResultV1>,
    packed_result: bool,
    enum_variant_names: SharedEnumVariantNames,
    next_cursor: Option<CursorToken>,
}

/// Owned symbolic-query response components in transport assembly order.
pub type SymbolicQueryResponseParts = (
    SymbolicQueryIdentity,
    String,
    u64,
    BTreeMap<String, SymbolicResultField>,
    Option<CoveredQueryResultV1>,
    bool,
    SharedEnumVariantNames,
    Option<CursorToken>,
);

impl ExecuteSymbolicQueryResult {
    /// Minimal success result for transport residual-stage harnesses.
    ///
    /// Not a semantic query outcome; fields are empty and identity is fixed
    /// fixture material used only by protocol residual instrumentation tests.
    /// Gated behind the `test-fixtures` feature so no shipped configuration
    /// exposes a synthetic success through the public service surface.
    #[cfg(feature = "test-fixtures")]
    #[must_use]
    pub fn transport_residual_fixture() -> Self {
        Self {
            identity: SymbolicQueryIdentity {
                lineage: ContractLineage::new("transport-residual")
                    .expect("fixture lineage is valid"),
                version: ContractVersion::new(1).expect("fixture version is valid"),
                bundle_hash: ContractBundleHash::from_bytes([0x11; 32]),
                name: Some("ResidualFixture".to_owned()),
                plan_hash: riffdb_types::QueryPlanHash::from_bytes([0x22; 32]),
                module_hash: None,
            },
            outcome: "Ok".to_owned(),
            application_head: 0,
            fields: BTreeMap::new(),
            compact_result: None,
            packed_result: false,
            enum_variant_names: Arc::new(BTreeMap::new()),
            next_cursor: None,
        }
    }

    pub(crate) fn from_snapshot(
        program: &QueryAccessProgramV1,
        snapshot: QueryOwnedSnapshot,
        enum_variant_names: SharedEnumVariantNames,
        retain_compact: bool,
        retain_packed: bool,
    ) -> Self {
        let application_head = snapshot.application_head();
        let outcome = snapshot.outcome().to_owned();
        let (mut fields, covered_result) = snapshot.into_result_parts();
        let compact_result = if retain_compact {
            covered_result
        } else {
            // Legacy/ad-hoc callers retain the exact symbolic response model.
            // Rebuild only here; the compact generated path never pays maps.
            if let Some(covered) = covered_result {
                let (name, value) = covered.into_named_value();
                fields.insert(name, value);
            }
            None
        };
        let fields = fields
            .into_iter()
            .map(|(name, value)| {
                let value = match value {
                    QueryResultValue::One(row) => {
                        SymbolicResultField::One(SymbolicResultRecord::from_row(row))
                    }
                    QueryResultValue::Maybe(row) => {
                        SymbolicResultField::Maybe(row.map(SymbolicResultRecord::from_row))
                    }
                    QueryResultValue::Many(rows) => SymbolicResultField::Many(
                        rows.into_iter()
                            .map(SymbolicResultRecord::from_row)
                            .collect(),
                    ),
                    QueryResultValue::AggregateOne(row) => {
                        SymbolicResultField::One(SymbolicResultRecord::from_aggregate(row))
                    }
                    QueryResultValue::AggregateMany(rows) => SymbolicResultField::Many(
                        rows.into_iter()
                            .map(SymbolicResultRecord::from_aggregate)
                            .collect(),
                    ),
                };
                (name, value)
            })
            .collect();
        let packed_result = retain_packed && compact_result.is_some();
        Self {
            identity: SymbolicQueryIdentity::from_program(program),
            outcome,
            application_head,
            fields,
            compact_result,
            packed_result,
            enum_variant_names,
            next_cursor: None,
        }
    }

    pub(crate) fn from_named_snapshot(
        program: &QueryAccessProgramV1,
        module_hash: QueryModuleHash,
        snapshot: QueryOwnedSnapshot,
        enum_variant_names: SharedEnumVariantNames,
    ) -> Self {
        let mut result = Self::from_snapshot(program, snapshot, enum_variant_names, false, false);
        result.identity = SymbolicQueryIdentity::from_named(program, module_hash);
        result
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

    /// Positional named result selected by representation negotiation.
    #[must_use]
    pub const fn compact_result(&self) -> Option<&CoveredQueryResultV1> {
        self.compact_result.as_ref()
    }

    /// Consumes the result into identity metadata and owned fields for transport.
    #[must_use]
    pub fn into_response_parts(self) -> SymbolicQueryResponseParts {
        (
            self.identity,
            self.outcome,
            self.application_head,
            self.fields,
            self.compact_result,
            self.packed_result,
            self.enum_variant_names,
            self.next_cursor,
        )
    }

    /// Test-only constructor for golden conversion fixtures and enum-map sharing tests.
    #[cfg(any(test, feature = "test-fixtures"))]
    #[doc(hidden)]
    pub fn from_parts_for_test(
        identity: SymbolicQueryIdentity,
        outcome: String,
        application_head: u64,
        fields: BTreeMap<String, SymbolicResultField>,
        enum_variant_names: SharedEnumVariantNames,
    ) -> Self {
        Self {
            identity,
            outcome,
            application_head,
            fields,
            compact_result: None,
            packed_result: false,
            enum_variant_names,
            next_cursor: None,
        }
    }

    /// Resolves a canonical enum identity to its contract source name.
    #[must_use]
    pub fn enum_variant_name(&self, type_id: u32, variant_id: u32) -> Option<&str> {
        self.enum_variant_names
            .get(&(type_id, variant_id))
            .map(String::as_str)
    }

    pub(crate) fn shared_enum_variant_names(&self) -> SharedEnumVariantNames {
        Arc::clone(&self.enum_variant_names)
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

    /// Returns one bounded policy-filtered symbolic application-catalog page.
    fn get_application_catalog(
        &self,
        context: RequestContext,
        request: ApplicationCatalogRequest,
    ) -> ServiceFuture<'_, ApplicationCatalogResult>;

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

    /// Compiles and publishes one immutable exact-contract reactive module.
    fn deploy_reactive_module(
        &self,
        context: RequestContext,
        request: DeployReactiveModuleRequest,
    ) -> ServiceFuture<'_, DeployReactiveModuleResult>;

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

    fn get_application_catalog(
        &self,
        context: RequestContext,
        request: ApplicationCatalogRequest,
    ) -> ServiceFuture<'_, ApplicationCatalogResult> {
        let service = Arc::clone(&self.inner);
        let ingress = context.ingress();
        self.spawn_operation(ServiceOperationV1::DescribeContract, ingress, async move {
            get_application_catalog(service, context, request).await
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

    fn deploy_reactive_module(
        &self,
        context: RequestContext,
        request: DeployReactiveModuleRequest,
    ) -> ServiceFuture<'_, DeployReactiveModuleResult> {
        let service = Arc::clone(&self.inner);
        let ingress = context.ingress();
        self.spawn_operation(
            ServiceOperationV1::DeployReactiveModule,
            ingress,
            async move { deploy_reactive_module(service, context, request).await },
        )
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

async fn get_application_catalog(
    service: Arc<RiffDbServiceInner>,
    context: RequestContext,
    request: ApplicationCatalogRequest,
) -> ServiceResult<ApplicationCatalogResult> {
    const OPERATION: ServiceOperationV1 = ServiceOperationV1::DescribeContract;
    let bundle = prepare_selected_contract(
        &service,
        &context,
        request.contract().selection(),
        OPERATION,
    )
    .await?;
    ensure_selected_hash(request.contract(), &bundle)?;
    let module =
        read_active_query_module_for_discovery(&service, &context, bundle.clone(), OPERATION)
            .await?;
    let candidates = ApplicationCatalogCandidatesV1::from_exact_application(
        bundle.bundle(),
        module.as_ref().map(ValidatedQueryModule::module),
    )
    .map_err(|_| service.internal_failure(OPERATION, InternalDefect::ProofMismatch))?;
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
    let authorities = candidates.authorities();
    let current_visibility = filter_application_catalog_visibility(
        &service,
        &context,
        &begun,
        bundle.bundle(),
        module.as_ref().map(ValidatedQueryModule::module),
        authorities.as_slice(),
    )
    .await?;
    let (_, completion) = begun.into_initial_authorization_and_completion();
    let shaped = (|| {
        let lookup = ApplicationCatalogCursorLookup::new(request.limit());
        let prior = match request.cursor() {
            Some(cursor) => match service.cursors.resolve_application_catalog(
                cursor,
                context.principal().principal_id(),
                &lookup,
            ) {
                Ok(state)
                    if state.contract_lineage() == bundle.lineage()
                        && state.contract_version() == bundle.contract_version()
                        && state.contract_hash() == bundle.bundle_hash()
                        && state.module_hash()
                            == module.as_ref().map(ValidatedQueryModule::identity)
                        && state.authority_visibility().len() == authorities.len() =>
                {
                    Some(state)
                }
                Ok(_) | Err(CursorAccessError::InvalidCursor) => {
                    return Err(application_catalog_invalid_cursor());
                }
                Err(CursorAccessError::Unavailable) => {
                    service
                        .providers
                        .telemetry
                        .record(ServiceTelemetryEvent::CursorUnavailable);
                    return Err(PublicError::storage_unavailable().into());
                }
            },
            None => None,
        };
        let effective_visibility = match prior.as_deref() {
            Some(prior) => current_visibility
                .iter()
                .zip(prior.authority_visibility())
                .map(|(current, prior)| *current && *prior)
                .collect::<Vec<_>>(),
            None => current_visibility,
        };
        let effective_limit = prior.as_deref().map_or(request.limit(), |prior| {
            request.limit().min(prior.effective_limit())
        });
        let after_candidate = prior.as_deref().map(|prior| prior.after_candidate());
        let visible = authorities
            .iter()
            .cloned()
            .zip(effective_visibility.iter().copied())
            .filter_map(|(authority, visible)| visible.then_some(authority))
            .collect::<BTreeSet<_>>();
        let (page, continuation_after_candidate) = candidates
            .authorized_page(
                &visible,
                after_candidate,
                usize::from(effective_limit.get()),
            )
            .map_err(|_| service.internal_failure(OPERATION, InternalDefect::ProofMismatch))?;
        let cursor_guard = if page.has_more() {
            let next_after = continuation_after_candidate.ok_or_else(|| {
                service.internal_failure(OPERATION, InternalDefect::ProofMismatch)
            })?;
            let state = ApplicationCatalogCursorState::new(
                next_after,
                bundle.lineage().clone(),
                bundle.contract_version(),
                bundle.bundle_hash(),
                module.as_ref().map(ValidatedQueryModule::identity),
                effective_visibility,
                effective_limit,
            )
            .map_err(|_| service.internal_failure(OPERATION, InternalDefect::ProofMismatch))?;
            Some(
                service
                    .cursors
                    .register_application_catalog_unpublished(
                        context.principal().principal_id(),
                        lookup,
                        state,
                    )
                    .map_err(|_| PublicError::storage_unavailable())?,
            )
        } else {
            None
        };
        let next_cursor = cursor_guard
            .as_ref()
            .map(crate::CursorPublicationGuard::token);
        Ok((ApplicationCatalogResult { page, next_cursor }, cursor_guard))
    })();
    let (result, cursor_guard) =
        finish_discovery_result(&service, &context, &completion, shaped).await?;
    if let Some(guard) = cursor_guard {
        guard.publish();
    }
    Ok(result)
}

async fn filter_application_catalog_visibility(
    service: &RiffDbServiceInner,
    context: &RequestContext,
    begun: &crate::orchestration::BegunInvocation,
    bundle: &riffdb_contract_ir::ContractBundle,
    module: Option<&riffdb_query_module::QueryModule>,
    authorities: &[ApplicationCatalogAuthorityV1],
) -> ServiceResult<Vec<bool>> {
    let query_candidates =
        application_catalog_query_candidates(bundle, module).ok_or_else(|| {
            service.internal_failure(
                ServiceOperationV1::DescribeContract,
                InternalDefect::ProofMismatch,
            )
        })?;
    let mut visibility = Vec::with_capacity(authorities.len());
    for batch in authorities.chunks(MAX_DISCOVERY_PAGE_ITEMS) {
        let authorization = begun.reauthorize(service, context).await?;
        let mut commands = Vec::new();
        let mut command_positions = Vec::new();
        let mut queries = Vec::new();
        let mut query_positions = Vec::new();
        let mut batch_visibility = vec![false; batch.len()];
        for (position, authority) in batch.iter().enumerate() {
            match authority {
                ApplicationCatalogAuthorityV1::Contract => batch_visibility[position] = true,
                ApplicationCatalogAuthorityV1::Command {
                    lineage,
                    command_id,
                } => {
                    command_positions.push(position);
                    commands.push(CommandToolCandidate::new(lineage.clone(), *command_id));
                }
                ApplicationCatalogAuthorityV1::Query {
                    lineage,
                    module_hash,
                    query_name,
                } => {
                    let candidate = query_candidates.get(query_name).ok_or_else(|| {
                        service.internal_failure(
                            ServiceOperationV1::DescribeContract,
                            InternalDefect::ProofMismatch,
                        )
                    })?;
                    if candidate.operation().lineage() != lineage
                        || candidate.operation().module_hash() != *module_hash
                    {
                        return Err(service.internal_failure(
                            ServiceOperationV1::DescribeContract,
                            InternalDefect::ProofMismatch,
                        ));
                    }
                    query_positions.push(position);
                    queries.push(candidate.clone());
                }
            }
        }
        let filtered = authorization
            .into_discovery()
            .and_then(|proof| proof.application_catalog(&commands, &queries))
            .map_err(|_| {
                service.internal_failure(
                    ServiceOperationV1::DescribeContract,
                    InternalDefect::ProofMismatch,
                )
            })?;
        for (position, observed) in command_positions
            .into_iter()
            .zip(filtered.command_operations())
        {
            batch_visibility[position] = *observed == DiscoveryVisibility::Visible;
        }
        for (position, observed) in query_positions
            .into_iter()
            .zip(filtered.named_query_operations())
        {
            batch_visibility[position] = *observed == DiscoveryVisibility::Visible;
        }
        visibility.extend(batch_visibility);
    }
    Ok(visibility)
}

fn application_catalog_query_candidates(
    bundle: &riffdb_contract_ir::ContractBundle,
    module: Option<&riffdb_query_module::QueryModule>,
) -> Option<BTreeMap<QueryOperationName, ApplicationCatalogQueryCandidate>> {
    let mut candidates = BTreeMap::new();
    let Some(module) = module else {
        return Some(candidates);
    };
    for query in module.queries() {
        let query_name = QueryOperationName::new(query.name().to_owned()).ok()?;
        let mut accesses = Vec::with_capacity(query.plan().authorization().len());
        for access in query.plan().authorization() {
            let entity = bundle
                .schema()
                .entities()
                .iter()
                .find(|entity| entity.name() == access.entity())?;
            let mut non_key_fields = access
                .internal_fields()
                .filter_map(|(_, field)| {
                    (!entity.primary_key_fields().contains(&field)).then_some(field)
                })
                .collect::<Vec<_>>();
            non_key_fields.sort_unstable();
            non_key_fields.dedup();
            let maximum_rows = NonZeroU16::new(u16::try_from(access.maximum_rows()).ok()?)?;
            accesses.push(
                ApplicationQueryAccessRequirement::new(
                    entity.id(),
                    None,
                    non_key_fields,
                    maximum_rows,
                )
                .ok()?,
            );
        }
        let candidate = ApplicationCatalogQueryCandidate::new(
            NamedQueryToolCandidate::new(
                bundle.lineage().clone(),
                module.identity(),
                query_name.clone(),
            ),
            accesses,
        )
        .ok()?;
        if candidates.insert(query_name, candidate).is_some() {
            return None;
        }
    }
    Some(candidates)
}

fn application_catalog_invalid_cursor() -> ServiceFailure {
    let issue = ValidationIssue::new(ValidationCode::InvalidValue, ValidationPath::root());
    PublicError::validation(ValidationIssues::one(issue)).into()
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

async fn deploy_reactive_module(
    service: Arc<RiffDbServiceInner>,
    context: RequestContext,
    request: DeployReactiveModuleRequest,
) -> ServiceResult<DeployReactiveModuleResult> {
    const OPERATION: ServiceOperationV1 = ServiceOperationV1::DeployReactiveModule;
    let bundle =
        prepare_selected_contract(&service, &context, request.contract.selection(), OPERATION)
            .await?;
    ensure_selected_hash(&request.contract, &bundle)?;
    let operation = OperationRequest::deploy_reactive_module(
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

    let mut query_modules = Vec::with_capacity(request.query_module_hashes.len());
    for hash in &request.query_module_hashes {
        match load_query_module(&service, &context, bundle.clone(), Some(*hash), OPERATION).await {
            Ok(Some(module)) => query_modules.push(module),
            Ok(None) => {
                let failure = application_validation_failure(
                    ValidationCode::InvalidValue,
                    ApplicationErrorCode::ModuleUnavailable,
                );
                return Err(finish_failure(&service, &context, &begun, failure).await);
            }
            Err(failure) => {
                return Err(finish_failure(&service, &context, &begun, failure).await);
            }
        }
    }
    let module = match ValidatedReactiveModule::compile(&request.source, &bundle, &query_modules) {
        Ok(module) => module,
        Err(_) => {
            let failure = application_validation_failure(
                ValidationCode::InvalidValue,
                ApplicationErrorCode::QueryInvalid,
            );
            return Err(finish_failure(&service, &context, &begun, failure).await);
        }
    };
    let descriptor = ReactiveModuleDescriptor::from_module(&module);
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
    let preparation = match ReactiveModulePublicationPreparation::new(
        context.request_id(),
        PreparedReactiveModulePublication::new(module),
        authorization,
    ) {
        Ok(preparation) => preparation,
        Err(_) => {
            let failure = service.internal_failure(OPERATION, InternalDefect::ProofMismatch);
            return Err(finish_failure(&service, &context, &begun, failure).await);
        }
    };
    let receipt = match permit.submit_reactive_module_publication(preparation) {
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
        CoordinatorReactiveModulePublicationOutcome::Published(_) => {
            ReactiveModuleDeploymentDisposition::Published
        }
        CoordinatorReactiveModulePublicationOutcome::AlreadyPublished(_) => {
            ReactiveModuleDeploymentDisposition::AlreadyPublished
        }
        CoordinatorReactiveModulePublicationOutcome::ModuleVersionConflict => {
            ReactiveModuleDeploymentDisposition::ModuleVersionConflict
        }
        CoordinatorReactiveModulePublicationOutcome::ContractUnavailable => {
            ReactiveModuleDeploymentDisposition::ContractUnavailable
        }
        CoordinatorReactiveModulePublicationOutcome::QueryModuleUnavailable(hash) => {
            ReactiveModuleDeploymentDisposition::QueryModuleUnavailable(hash)
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
    Ok(DeployReactiveModuleResult {
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
        query: Box::new(CheckedSymbolicQuery::from_named_query(
            query,
            module.identity(),
        )),
        lines: query.explain_lines(),
    };
    begun.reauthorize(&service, &context).await?;
    finish_success(&service, &context, &begun).await?;
    Ok(result)
}

/// Records one closed read-pipeline stage that ended now.
///
/// The stage sinks are behind a trait object, so the caller pays one virtual
/// call and one histogram update. `Instant::elapsed` is the only clock read.
fn record_read_stage(
    service: &RiffDbServiceInner,
    stage: ReadPipelineStage,
    started: std::time::Instant,
) {
    service
        .providers
        .telemetry
        .record(ServiceTelemetryEvent::ReadPipelineStageCompleted {
            stage,
            elapsed: started.elapsed(),
        });
}

async fn execute_named_query(
    service: Arc<RiffDbServiceInner>,
    context: RequestContext,
    request: NamedSymbolicQueryRequest,
) -> ServiceResult<ExecuteSymbolicQueryResult> {
    const OPERATION: ServiceOperationV1 = ServiceOperationV1::ExecuteQuery;
    let plan_lookup_started = Instant::now();
    let query_name = QueryOperationName::new(request.query_name.clone())
        .map_err(|_| validation_failure(ValidationCode::InvalidValue))?;
    let fast_resolution = match (
        service.providers.query_modules.as_ref(),
        request.contract.exact_identity(),
        request.module_hash,
    ) {
        (Some(modules), Some((lineage, version, contract_hash)), Some(module_hash)) => {
            match modules.prepare_exact_named_query(
                context.control(),
                crate::ExactNamedQueryRequest::new(
                    lineage.clone(),
                    version,
                    contract_hash,
                    module_hash,
                    query_name.clone(),
                ),
            ) {
                Ok(resolution) => resolution,
                Err(crate::QueryModuleReadError::Unavailable) => {
                    return Err(PublicError::storage_unavailable().into());
                }
                Err(crate::QueryModuleReadError::Integrity) => {
                    return Err(service.internal_failure(OPERATION, InternalDefect::ProofMismatch));
                }
            }
        }
        _ => None,
    };
    let (bundle, module, query_index) = if let Some(resolution) = fast_resolution {
        let (bundle, module, query_index) = resolution.into_parts();
        ensure_selected_hash(&request.contract, &bundle)?;
        (bundle, module, query_index)
    } else {
        let bundle =
            prepare_selected_contract(&service, &context, request.contract.selection(), OPERATION)
                .await?;
        ensure_selected_hash(&request.contract, &bundle)?;
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
        let query_index = module
            .module()
            .queries()
            .binary_search_by(|query| query.name().cmp(request.query_name.as_str()))
            .map_err(|_| {
                application_validation_failure(
                    ValidationCode::InvalidValue,
                    ApplicationErrorCode::QueryUnavailable,
                )
            })?;
        (bundle, module, query_index)
    };
    let module_hash = module.identity();
    let query = &module.module().queries()[query_index];
    service
        .providers
        .telemetry
        .record(ServiceTelemetryEvent::ReadPipelineStageCompleted {
            stage: ReadPipelineStage::PlanLookup,
            elapsed: plan_lookup_started.elapsed(),
        });
    let exact_predicate = query
        .shared_exact_predicate_result()
        .map(ExactPredicateNamedQuery::PresentOnly)
        .or_else(|| {
            query
                .shared_nullable_exact_predicate_result()
                .map(ExactPredicateNamedQuery::Nullable)
        });
    if let Some(exact) = exact_predicate {
        return execute_exact_predicate_named_query(
            service,
            context,
            bundle,
            exact,
            query.shared_document(),
            module_hash,
            query_name,
            request.parameters,
            request.cursor,
            request.minimum_application_head,
            request.consistency,
        )
        .await;
    }
    if let Some(exact) = query.shared_exact_text_result() {
        return execute_exact_named_query(
            service,
            context,
            bundle,
            exact,
            query.shared_document(),
            module_hash,
            query_name,
            request.parameters,
            request.cursor,
            request.minimum_application_head,
            request.consistency,
        )
        .await;
    }
    if let Some(tokenized) = query.shared_tokenized_text_result() {
        return execute_tokenized_named_query(
            service,
            context,
            bundle,
            tokenized,
            query.shared_document(),
            module_hash,
            query_name,
            request.parameters,
            request.cursor,
            request.minimum_application_head,
            request.consistency,
        )
        .await;
    }
    let presence = query
        .operational_family()
        .map(|family| {
            family
                .presence_parameters()
                .iter()
                .map(|name| {
                    request
                        .parameters
                        .get(name)
                        .is_some_and(|value| !matches!(value, SubmittedValue::Null))
                })
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    let order_program = query.order_family().and_then(|family| {
        let submitted = request.parameters.get(family.selector_parameter())?;
        let member = match submitted {
            SubmittedValue::Enum(value) => match (value.type_id(), value.variant_id()) {
                (Some(type_id), Some(variant_id)) => family.select(type_id, variant_id),
                _ => value
                    .name()
                    .and_then(|name| family.select_name(name.as_str())),
            },
            SubmittedValue::String(value) => family.select_name(value.as_str()),
            _ => None,
        }?;
        Some(member.shared_program())
    });
    let program = order_program
        .or_else(|| query.select_program(&presence))
        .ok_or_else(|| {
            application_validation_failure(
                ValidationCode::InvalidValue,
                ApplicationErrorCode::QueryInvalid,
            )
        })?;
    let (response_plan_hash, cursor_plan_hash) = named_query_plan_hashes(query, &program);
    if program.projected_source().is_some() {
        let authorization_plan_hash = program.identity().hash();
        return execute_projected_vector_named_query(
            service,
            context,
            bundle,
            program,
            query.shared_document(),
            module_hash,
            authorization_plan_hash,
            response_plan_hash,
            query_name,
            request.parameters,
            request.cursor,
            request.minimum_application_head,
            request.consistency,
        )
        .await;
    }
    if program
        .steps()
        .iter()
        .any(|step| matches!(step.access(), QueryAccessKind::Nearest { .. }))
    {
        return Err(application_validation_failure(
            ValidationCode::InvalidValue,
            ApplicationErrorCode::ProjectedSourceRequired,
        ));
    }
    let aggregates: Arc<[riffdb_query_ir::OperationalAggregateV1]> = query
        .operational_family()
        .map_or_else(|| Arc::from([]), |family| Arc::from(family.aggregates()));
    execute_compiled_query(
        service,
        context,
        bundle,
        program,
        aggregates,
        query.shared_document(),
        Some(module_hash),
        Some(response_plan_hash),
        Some(cursor_plan_hash),
        QueryAuthority::Named {
            module_hash,
            query_name,
        },
        request.parameters,
        request.cursor,
        request.minimum_application_head,
        request.consistency,
        request.accepts_compact_result_v1,
        request.accepts_packed_result_v1,
    )
    .await
}

/// Separates the stable public named-operation identity from the selected
/// order member used to bind continuation state. The generated application
/// catalog pins the complete finite family; the cursor additionally pins the
/// immutable member selected by the closed enum parameter.
fn named_query_plan_hashes(
    query: &riffdb_query_module::CompiledNamedQuery,
    program: &QueryAccessProgramV1,
) -> (QueryPlanHash, QueryPlanHash) {
    let response = query.plan().identity();
    let cursor = if query.order_family().is_some() {
        program.identity().hash()
    } else {
        response
    };
    (response, cursor)
}

#[allow(clippy::too_many_arguments)]
async fn execute_projected_vector_named_query(
    service: Arc<RiffDbServiceInner>,
    context: RequestContext,
    bundle: riffdb_catalog::ValidatedContractBundle,
    program: Arc<QueryAccessProgramV1>,
    document: Arc<Document>,
    module_hash: QueryModuleHash,
    authorization_plan_hash: QueryPlanHash,
    response_plan_hash: QueryPlanHash,
    query_name: QueryOperationName,
    submitted: SymbolicQueryParameters,
    cursor: Option<CursorToken>,
    minimum_application_head: Option<u64>,
    consistency: Option<QueryConsistencyV1>,
) -> ServiceResult<ExecuteSymbolicQueryResult> {
    const OPERATION: ServiceOperationV1 = ServiceOperationV1::ExecuteQuery;
    if cursor.is_some() || program.steps().len() != 1 {
        return Err(application_validation_failure(
            ValidationCode::InvalidValue,
            ApplicationErrorCode::CursorInvalid,
        ));
    }
    let param_materialize_started = Instant::now();
    let source = program
        .projected_source()
        .ok_or_else(|| service.internal_failure(OPERATION, InternalDefect::ProofMismatch))?;
    let parameters = materialize_query_parameters(
        &service,
        OPERATION,
        bundle.bundle(),
        document.as_ref(),
        &submitted,
    )?;
    let target = application_query_target_with_identity(
        bundle.bundle(),
        &program,
        &parameters,
        context.ingress(),
        authorization_plan_hash,
        program.cost(),
        None,
    )
    .ok_or_else(|| validation_failure(ValidationCode::InvalidValue))?;
    let operation_request = OperationRequest::execute_named_query(
        bundle.lineage().clone(),
        module_hash,
        query_name,
        target,
    )
    .map_err(|_| service.internal_failure(OPERATION, InternalDefect::ProofMismatch))?;
    record_read_stage(
        &service,
        ReadPipelineStage::ParamMaterialize,
        param_materialize_started,
    );
    let authorize_begin_started = Instant::now();
    let begun = begin_symbolic(&service, &context, &bundle, operation_request, OPERATION).await?;
    record_read_stage(
        &service,
        ReadPipelineStage::AuthorizeBegin,
        authorize_begin_started,
    );
    let authorize_pre_started = Instant::now();
    let execution_authorization = begun
        .reauthorize_read(&service, &context)
        .await?
        .into_application_query()
        .ok_or_else(|| service.internal_failure(OPERATION, InternalDefect::ProofMismatch))?;
    let minimum_application_head = match resolve_admission_head_floor(
        &service,
        &context,
        minimum_application_head,
        consistency,
    )
    .await
    {
        Ok(floor) => floor,
        Err(failure) => return Err(finish_failure(&service, &context, &begun, failure).await),
    };
    let row_policy =
        resolve_authorized_query_row_policy_context(&execution_authorization, bundle.bundle())
            .map_err(|_| service.internal_failure(OPERATION, InternalDefect::ProofMismatch))?
            .map(Arc::new);
    let policy_shape = execution_authorization.application_role_hash();
    let row_policy_identity = row_policy
        .as_deref()
        .map(|policy| {
            policy
                .internal_capability_identity()
                .ok_or_else(|| service.internal_failure(OPERATION, InternalDefect::ProofMismatch))
        })
        .transpose()?;
    let step = &program.steps()[0];
    let entity = bundle
        .bundle()
        .schema()
        .entity(source.internal_entity_id())
        .ok_or_else(|| service.internal_failure(OPERATION, InternalDefect::ProofMismatch))?;
    let vector_spec = bundle
        .bundle()
        .schema()
        .vector_field_spec(source.internal_entity_id(), source.internal_field_id())
        .ok_or_else(|| service.internal_failure(OPERATION, InternalDefect::ProofMismatch))?;
    let production = bundle
        .bundle()
        .schema()
        .vector_production_spec(source.internal_entity_id(), source.internal_field_id())
        .ok_or_else(|| service.internal_failure(OPERATION, InternalDefect::ProofMismatch))?;
    let partition_value = parameters
        .get(program.partition_parameter())
        .cloned()
        .ok_or_else(|| validation_failure(ValidationCode::InvalidValue))?;
    let aggregate = bundle
        .bundle()
        .schema()
        .aggregate_for_entity(source.internal_entity_id())
        .ok_or_else(|| service.internal_failure(OPERATION, InternalDefect::ProofMismatch))?;
    let partition = aggregate
        .keys()
        .partition_schema()
        .encode_partition(std::slice::from_ref(&partition_value))
        .map_err(|_| validation_failure(ValidationCode::InvalidValue))?;
    let (vector_parameter, maximum_k) = match step.access() {
        QueryAccessKind::Nearest {
            vector_parameter,
            k,
            ..
        } => (vector_parameter, *k),
        _ => return Err(service.internal_failure(OPERATION, InternalDefect::ProofMismatch)),
    };
    let query_vector = match parameters.get(vector_parameter) {
        Some(CanonicalValue::Vector(vector)) => vector.clone(),
        _ => return Err(validation_failure(ValidationCode::TypeMismatch)),
    };
    let k = match step.row_limit() {
        riffdb_query_ir::QueryRowLimit::Literal(value) => *value,
        riffdb_query_ir::QueryRowLimit::Parameter { name, default } => parameters
            .get(name)
            .and_then(|value| match value {
                CanonicalValue::U64(value) => Some(*value),
                _ => None,
            })
            .or(*default)
            .ok_or_else(|| validation_failure(ValidationCode::InvalidValue))?,
        riffdb_query_ir::QueryRowLimit::BoundedParameter {
            name,
            maximum,
            default,
        } => parameters
            .get(name)
            .and_then(|value| match value {
                CanonicalValue::U64(value) if *value > 0 && *value <= *maximum => Some(*value),
                _ => None,
            })
            .or(*default)
            .filter(|value| *value > 0 && *value <= *maximum)
            .ok_or_else(|| validation_failure(ValidationCode::InvalidValue))?,
        riffdb_query_ir::QueryRowLimit::CandidateComplete { .. } => {
            return Err(service.internal_failure(OPERATION, InternalDefect::ProofMismatch));
        }
    };
    let k = u32::try_from(k)
        .ok()
        .filter(|k| *k > 0 && *k <= maximum_k)
        .ok_or_else(|| validation_failure(ValidationCode::InvalidValue))?;
    let access = program
        .internal_entity_access(step.entity())
        .ok_or_else(|| service.internal_failure(OPERATION, InternalDefect::ProofMismatch))?;
    let predicates = step
        .predicates()
        .iter()
        .map(|predicate| {
            if predicate.operator() != QueryPredicateOperator::Equal {
                return None;
            }
            let QueryPredicateValue::Parameter(parameter) = predicate.value() else {
                return None;
            };
            Some(ColumnPredicate::Eq {
                field: access.internal_field_id(predicate.field())?,
                value: parameters.get(parameter)?.clone(),
            })
        })
        .collect::<Option<Vec<_>>>()
        .ok_or_else(|| service.internal_failure(OPERATION, InternalDefect::ProofMismatch))?;
    let admission_head_fenced = consistency == Some(QueryConsistencyV1::AdmissionHead);
    let inherited_minimum_epoch = match source.freshness() {
        ProjectedVectorFreshnessV1::Available => None,
        ProjectedVectorFreshnessV1::Causal {
            inherit_session_commit,
            ..
        } if inherit_session_commit => minimum_application_head,
        ProjectedVectorFreshnessV1::Causal { .. } | ProjectedVectorFreshnessV1::Bounded { .. } => {
            None
        }
    };
    let minimum_epoch = admission_head_fenced
        .then_some(minimum_application_head)
        .flatten()
        .or(inherited_minimum_epoch)
        .map(|value| {
            CommitSequence::new(value)
                .ok_or_else(|| validation_failure(ValidationCode::InvalidValue))
        })
        .transpose()?;
    let max_lag_ms = match source.freshness() {
        ProjectedVectorFreshnessV1::Bounded { max_lag_ms } => Some(max_lag_ms),
        ProjectedVectorFreshnessV1::Available | ProjectedVectorFreshnessV1::Causal { .. } => None,
    };
    let request = VectorProjectionRequest::new(
        source.name(),
        bundle.lineage().clone(),
        partition,
        partition_value,
        source.internal_entity_id(),
        source.internal_field_id(),
        production.metadata().clone(),
        vector_spec.stale_entity_count_threshold(),
        query_vector,
        k,
        vector_spec.metric(),
        predicates,
        row_policy,
        minimum_epoch,
        max_lag_ms,
    );
    let Some(provider) = service.providers.vector_projection.as_ref() else {
        let failure = PublicError::storage_unavailable().into();
        return Err(finish_failure(&service, &context, &begun, failure).await);
    };
    record_read_stage(
        &service,
        ReadPipelineStage::AuthorizePre,
        authorize_pre_started,
    );
    let execute_started = Instant::now();
    let observed = match execute_vector_projection_with_freshness(
        &service,
        &context,
        &begun,
        provider.as_ref(),
        request,
        &source.freshness(),
        admission_head_fenced,
    )
    .await
    {
        Ok(result) => result,
        Err(failure) => {
            return Err(finish_failure(&service, &context, &begun, failure).await);
        }
    };
    record_read_stage(&service, ReadPipelineStage::Execute, execute_started);
    let authorize_post_started = Instant::now();
    // Safe point 3: no ranked values cross the boundary under stale authority.
    let release_authorization = begun
        .reauthorize_read(&service, &context)
        .await?
        .into_application_query()
        .ok_or_else(|| service.internal_failure(OPERATION, InternalDefect::ProofMismatch))?;
    let release_policy =
        resolve_authorized_query_row_policy_context(&release_authorization, bundle.bundle())
            .map_err(|_| service.internal_failure(OPERATION, InternalDefect::ProofMismatch))?;
    let release_policy_identity = release_policy
        .as_ref()
        .map(|policy| {
            policy
                .internal_capability_identity()
                .ok_or_else(|| service.internal_failure(OPERATION, InternalDefect::ProofMismatch))
        })
        .transpose()?;
    if release_authorization.application_role_hash() != policy_shape
        || release_policy_identity != row_policy_identity
    {
        return Err(begun.finish_authorization_denial(&service, &context).await);
    }
    record_read_stage(
        &service,
        ReadPipelineStage::AuthorizePost,
        authorize_post_started,
    );
    let response_build_started = Instant::now();
    let result = vector_projection_response(
        &program,
        document.as_ref(),
        module_hash,
        response_plan_hash,
        observed,
        Arc::clone(bundle.enum_variant_names()),
        entity,
    )
    .ok_or_else(|| service.internal_failure(OPERATION, InternalDefect::ProofMismatch))?;
    record_read_stage(
        &service,
        ReadPipelineStage::ResponseBuild,
        response_build_started,
    );
    let audit_finish_started = Instant::now();
    finish_success(&service, &context, &begun).await?;
    record_read_stage(
        &service,
        ReadPipelineStage::AuditFinish,
        audit_finish_started,
    );
    Ok(result)
}

async fn execute_vector_projection_with_freshness(
    service: &RiffDbServiceInner,
    context: &RequestContext,
    begun: &crate::orchestration::BegunInvocation,
    provider: &dyn crate::VectorProjectionPort,
    request: VectorProjectionRequest,
    freshness: &ProjectedVectorFreshnessV1,
    admission_head_fenced: bool,
) -> ServiceResult<crate::VectorProjectionResult> {
    const MAX_OBSERVATIONS: usize = 64;
    let max_wait = if admission_head_fenced {
        EXACT_PROVIDER_READINESS_WAIT
    } else if let ProjectedVectorFreshnessV1::Causal { max_wait_ms, .. } = freshness {
        Duration::from_millis(u64::from(*max_wait_ms))
    } else {
        return provider.execute(request).map_err(|error| {
            vector_projection_failure(service, ServiceOperationV1::ExecuteQuery, error)
        });
    };
    if request.minimum_epoch().is_none() {
        return provider.execute(request).map_err(|error| {
            vector_projection_failure(service, ServiceOperationV1::ExecuteQuery, error)
        });
    }
    let Some(columnar) = service.providers.columnar.as_ref() else {
        return Err(PublicError::storage_unavailable().into());
    };
    let wait_deadline = (Instant::now() + max_wait).min(context.control().deadline());
    let cancellation = columnar.notifier().cancellation();

    for _ in 0..MAX_OBSERVATIONS {
        if context.control().is_cancelled() {
            return Err(ServiceFailure::Cancelled);
        }
        let registration = columnar
            .notifier()
            .register(request.source_name().to_owned())
            .map_err(|error| match error.kind() {
                crate::columnar_notification::ColumnarNotificationErrorKind::WaiterCapacityExceeded => {
                    PublicError::storage_unavailable().into()
                }
                crate::columnar_notification::ColumnarNotificationErrorKind::Integrity => {
                    service.internal_failure(
                        ServiceOperationV1::ExecuteQuery,
                        InternalDefect::LowerIntegrity,
                    )
                }
            })?;
        match provider.execute(request.clone()) {
            Ok(result) => {
                drop(registration);
                return Ok(result);
            }
            Err(
                VectorProjectionPortError::FreshnessUnsatisfied
                | VectorProjectionPortError::Building
                | VectorProjectionPortError::Rebuilding,
            ) if Instant::now() < wait_deadline => {}
            Err(error) => {
                drop(registration);
                return Err(vector_projection_failure(
                    service,
                    ServiceOperationV1::ExecuteQuery,
                    error,
                ));
            }
        }
        let wake = registration
            .wait_controlled(wait_deadline, &cancellation)
            .map_err(|_| {
                service.internal_failure(
                    ServiceOperationV1::ExecuteQuery,
                    InternalDefect::LowerIntegrity,
                )
            })?;
        match wake {
            crate::ColumnarWake::Cancelled => return Err(ServiceFailure::Cancelled),
            crate::ColumnarWake::TimedOut => {
                return Err(vector_projection_failure(
                    service,
                    ServiceOperationV1::ExecuteQuery,
                    VectorProjectionPortError::FreshnessUnsatisfied,
                ));
            }
            crate::ColumnarWake::Notified => {
                begun.reauthorize_read(service, context).await?;
            }
        }
    }
    Err(vector_projection_failure(
        service,
        ServiceOperationV1::ExecuteQuery,
        VectorProjectionPortError::FreshnessUnsatisfied,
    ))
}

fn vector_projection_failure(
    service: &RiffDbServiceInner,
    operation: ServiceOperationV1,
    error: VectorProjectionPortError,
) -> ServiceFailure {
    let code = match error {
        VectorProjectionPortError::FreshnessUnsatisfied => {
            Some(ApplicationErrorCode::FreshnessUnsatisfied)
        }
        VectorProjectionPortError::Degraded => Some(ApplicationErrorCode::QueryUnavailable),
        VectorProjectionPortError::Building
        | VectorProjectionPortError::Rebuilding
        | VectorProjectionPortError::Unavailable => Some(ApplicationErrorCode::QueryUnavailable),
        VectorProjectionPortError::Integrity => None,
    };
    code.map_or_else(
        || service.internal_failure(operation, InternalDefect::ProofMismatch),
        |code| application_validation_failure(ValidationCode::InvalidValue, code),
    )
}

fn vector_projection_response(
    program: &QueryAccessProgramV1,
    document: &Document,
    module_hash: QueryModuleHash,
    plan_hash: QueryPlanHash,
    observed: crate::VectorProjectionResult,
    enum_variant_names: SharedEnumVariantNames,
    entity: &riffdb_contract_ir::EntitySchema,
) -> Option<ExecuteSymbolicQueryResult> {
    let step = program.steps().first()?;
    let result_name = selection_result_name(
        &document.body.selection,
        &document.body.bindings[0].name.value,
    )?;
    let (nearest, frontier, _head) = observed.into_parts();
    let mut names = BTreeMap::new();
    for name in step.selected_fields() {
        let field = entity
            .record()
            .fields()
            .iter()
            .find(|field| field.name() == name)?;
        names.insert(field.id(), name.as_str());
    }
    let rows = nearest
        .rows
        .into_iter()
        .map(|row| {
            let mut fields = BTreeMap::new();
            for (field, value) in nearest
                .primary_key_fields
                .iter()
                .zip(row.primary_key)
                .chain(nearest.projected_fields.iter().zip(row.cells))
            {
                if let Some(name) = names.get(field) {
                    fields.insert(Arc::<str>::from(*name), value);
                }
            }
            (fields.len() == names.len()).then_some(SymbolicResultRecord {
                entity: Arc::from(step.entity()),
                fields,
                nested: BTreeMap::new(),
                exact_decimals: BTreeMap::new(),
                exact_means: BTreeMap::new(),
            })
        })
        .collect::<Option<Vec<_>>>()?;
    let FrontierPosition::AppliedThrough(epoch) = frontier.position() else {
        return None;
    };
    Some(ExecuteSymbolicQueryResult {
        identity: SymbolicQueryIdentity::from_named_plan(program, module_hash, plan_hash),
        outcome: document.body.outcome.as_ref()?.value.as_str().to_owned(),
        application_head: epoch.get(),
        fields: BTreeMap::from([(result_name, SymbolicResultField::Many(rows))]),
        compact_result: None,
        packed_result: false,
        enum_variant_names,
        next_cursor: None,
    })
}

#[allow(clippy::too_many_arguments)]
async fn execute_exact_predicate_named_query(
    service: Arc<RiffDbServiceInner>,
    context: RequestContext,
    bundle: riffdb_catalog::ValidatedContractBundle,
    exact: ExactPredicateNamedQuery,
    document: Arc<Document>,
    module_hash: QueryModuleHash,
    query_name: QueryOperationName,
    submitted: SymbolicQueryParameters,
    cursor: Option<CursorToken>,
    minimum_application_head: Option<u64>,
    consistency: Option<QueryConsistencyV1>,
) -> ServiceResult<ExecuteSymbolicQueryResult> {
    const OPERATION: ServiceOperationV1 = ServiceOperationV1::ExecuteQuery;
    if cursor.is_some() {
        return Err(application_validation_failure(
            ValidationCode::InvalidValue,
            ApplicationErrorCode::CursorInvalid,
        ));
    }
    let param_materialize_started = Instant::now();
    let submitted = exact_parameters_with_defaults(
        document.as_ref(),
        submitted,
        [exact.limit_parameter(), exact.offset_parameter()],
    )
    .ok_or_else(|| validation_failure(ValidationCode::InvalidValue))?;
    let access_program = exact.representative_program();
    let parameters = materialize_query_parameters(
        &service,
        OPERATION,
        bundle.bundle(),
        document.as_ref(),
        &submitted,
    )?;
    let target = application_query_target_with_identity(
        bundle.bundle(),
        access_program,
        &parameters,
        context.ingress(),
        exact.identity(),
        exact.authorization_cost(),
        Some(NonZeroU16::MIN),
    )
    .ok_or_else(|| validation_failure(ValidationCode::InvalidValue))?;
    let partition_value = parameters
        .get(access_program.partition_parameter())
        .cloned()
        .ok_or_else(|| service.internal_failure(OPERATION, InternalDefect::ProofMismatch))?;
    let limit = exact_u64_parameter(&document, &parameters, exact.limit_parameter())
        .and_then(|value| u16::try_from(value).ok())
        .and_then(NonZeroU16::new)
        .filter(|limit| limit.get() <= exact.max_limit())
        .ok_or_else(|| validation_failure(ValidationCode::InvalidValue))?;
    let offset = exact_u64_parameter(&document, &parameters, exact.offset_parameter())
        .and_then(|value| u32::try_from(value).ok())
        .filter(|offset| *offset <= exact.max_offset())
        .ok_or_else(|| validation_failure(ValidationCode::InvalidValue))?;
    let exact_parameters = exact
        .value_parameters()
        .iter()
        .enumerate()
        .filter_map(|(ordinal, name)| {
            let value = parameters.get(name)?;
            (!matches!(value, CanonicalValue::Null)).then_some((ordinal, value))
        })
        .map(|(ordinal, value)| {
            u16::try_from(ordinal)
                .map_err(|_| validation_failure(ValidationCode::InvalidValue))
                .and_then(|ordinal| {
                    exact_parameter_value(value)
                        .map(|value| (ordinal, value))
                        .ok_or_else(|| validation_failure(ValidationCode::TypeMismatch))
                })
        })
        .collect::<ServiceResult<BTreeMap<_, _>>>()?;
    let presence_bits = exact.presence_parameters().iter().enumerate().try_fold(
        0_u64,
        |bits, (ordinal, name)| {
            let present = parameters
                .get(name)
                .is_some_and(|value| !matches!(value, CanonicalValue::Null));
            if !present {
                return Ok(bits);
            }
            let shift = u32::try_from(ordinal)
                .map_err(|_| validation_failure(ValidationCode::InvalidValue))?;
            bits.checked_add(
                1_u64
                    .checked_shl(shift)
                    .ok_or_else(|| validation_failure(ValidationCode::InvalidValue))?,
            )
            .ok_or_else(|| validation_failure(ValidationCode::InvalidValue))
        },
    )?;
    let member = exact
        .members()
        .iter()
        .copied()
        .find(|member| member.presence_bits() == presence_bits && member.order_ordinal() == 0)
        .ok_or_else(|| validation_failure(ValidationCode::InvalidValue))?;
    let operation_request = OperationRequest::execute_named_query(
        bundle.lineage().clone(),
        module_hash,
        query_name,
        target,
    )
    .map_err(|_| service.internal_failure(OPERATION, InternalDefect::ProofMismatch))?;
    record_read_stage(
        &service,
        ReadPipelineStage::ParamMaterialize,
        param_materialize_started,
    );
    let authorize_begin_started = Instant::now();
    let begun = begin_symbolic(&service, &context, &bundle, operation_request, OPERATION).await?;
    record_read_stage(
        &service,
        ReadPipelineStage::AuthorizeBegin,
        authorize_begin_started,
    );
    let authorize_pre_started = Instant::now();
    let execution_authorization = begun
        .reauthorize_read(&service, &context)
        .await?
        .into_application_query()
        .ok_or_else(|| service.internal_failure(OPERATION, InternalDefect::ProofMismatch))?;
    let minimum_application_head = match resolve_admission_head_floor(
        &service,
        &context,
        minimum_application_head,
        consistency,
    )
    .await
    {
        Ok(floor) => floor,
        Err(failure) => return Err(finish_failure(&service, &context, &begun, failure).await),
    };
    let minimum_epoch = minimum_application_head
        .map(|value| {
            CommitSequence::new(value)
                .ok_or_else(|| validation_failure(ValidationCode::InvalidValue))
        })
        .transpose()?;
    let row_policy =
        resolve_authorized_query_row_policy_context(&execution_authorization, bundle.bundle())
            .map_err(|_| service.internal_failure(OPERATION, InternalDefect::ProofMismatch))?
            .map(Arc::new);
    let row_policy_identity = row_policy
        .as_deref()
        .map(|policy| {
            policy
                .internal_capability_identity()
                .ok_or_else(|| service.internal_failure(OPERATION, InternalDefect::ProofMismatch))
        })
        .transpose()?;
    let Some(policy_shape) = execution_authorization.application_role_hash() else {
        let failure = application_validation_failure(
            ValidationCode::InvalidValue,
            ApplicationErrorCode::QueryUnavailable,
        );
        return Err(finish_failure(&service, &context, &begun, failure).await);
    };
    let Some(provider) = service.providers.exact_predicate.as_ref() else {
        let failure = PublicError::storage_unavailable().into();
        return Err(finish_failure(&service, &context, &begun, failure).await);
    };
    let provider_request = exact.provider_request(
        execution_authorization
            .target()
            .partition()
            .ok_or_else(|| service.internal_failure(OPERATION, InternalDefect::ProofMismatch))?
            .partition_key()
            .clone(),
        partition_value,
        policy_shape,
        row_policy,
        exact_parameters,
        member,
        offset,
        limit,
        minimum_epoch,
    );
    record_read_stage(
        &service,
        ReadPipelineStage::AuthorizePre,
        authorize_pre_started,
    );
    let execute_started = Instant::now();
    let observed = match execute_exact_provider_with_readiness(&service, &context, &begun, || {
        provider_request.execute(provider.as_ref())
    })
    .await?
    {
        Ok(observed) => observed,
        Err(error) => {
            let Some(code) = exact_projection_application_code(error) else {
                let failure = service.internal_failure(OPERATION, InternalDefect::ProofMismatch);
                return Err(finish_failure(&service, &context, &begun, failure).await);
            };
            let failure = application_validation_failure(ValidationCode::InvalidValue, code);
            return Err(finish_failure(&service, &context, &begun, failure).await);
        }
    };
    record_read_stage(&service, ReadPipelineStage::Execute, execute_started);
    let descriptor = exact
        .provider_descriptor_digest()
        .map_err(|_| service.internal_failure(OPERATION, InternalDefect::ProofMismatch))?;
    if observed.provider() != descriptor
        || minimum_epoch.is_some_and(|minimum| observed.epoch() < minimum)
    {
        let failure = service.internal_failure(OPERATION, InternalDefect::ProofMismatch);
        return Err(finish_failure(&service, &context, &begun, failure).await);
    }
    let authorize_post_started = Instant::now();
    let release_authorization = begun
        .reauthorize_read(&service, &context)
        .await?
        .into_application_query()
        .ok_or_else(|| service.internal_failure(OPERATION, InternalDefect::ProofMismatch))?;
    let release_row_policy =
        resolve_authorized_query_row_policy_context(&release_authorization, bundle.bundle())
            .map_err(|_| service.internal_failure(OPERATION, InternalDefect::ProofMismatch))?;
    let release_row_policy_identity = release_row_policy
        .as_ref()
        .map(|policy| {
            policy
                .internal_capability_identity()
                .ok_or_else(|| service.internal_failure(OPERATION, InternalDefect::ProofMismatch))
        })
        .transpose()?;
    if release_authorization.application_role_hash() != Some(policy_shape)
        || release_row_policy_identity != row_policy_identity
    {
        let failure = application_validation_failure(
            ValidationCode::InvalidValue,
            ApplicationErrorCode::QueryUnavailable,
        );
        return Err(finish_failure(&service, &context, &begun, failure).await);
    }
    record_read_stage(
        &service,
        ReadPipelineStage::AuthorizePost,
        authorize_post_started,
    );
    let response_build_started = Instant::now();
    let result = exact_predicate_result_response(
        &exact,
        &document,
        module_hash,
        observed,
        Arc::clone(bundle.enum_variant_names()),
    )
    .ok_or_else(|| service.internal_failure(OPERATION, InternalDefect::ProofMismatch))?;
    record_read_stage(
        &service,
        ReadPipelineStage::ResponseBuild,
        response_build_started,
    );
    let audit_finish_started = Instant::now();
    finish_success(&service, &context, &begun).await?;
    record_read_stage(
        &service,
        ReadPipelineStage::AuditFinish,
        audit_finish_started,
    );
    Ok(result)
}

enum ExactPredicateNamedQuery {
    PresentOnly(Arc<CompiledExactPredicateResultSetV1>),
    Nullable(Arc<CompiledNullableExactPredicateResultSetV1>),
}

#[derive(Clone)]
enum ExactPredicateProviderRequest {
    PresentOnly(ExactPredicateProjectionRequest),
    Nullable(NullableExactPredicateProjectionRequest),
}

impl ExactPredicateProviderRequest {
    fn execute(
        &self,
        provider: &dyn crate::ExactPredicateProjectionPort,
    ) -> Result<ExactPredicateProjectionResult, ExactTextProjectionPortError> {
        match self {
            Self::PresentOnly(request) => provider.execute(request.clone()),
            Self::Nullable(request) => provider.execute_nullable(request.clone()),
        }
    }
}

impl ExactPredicateNamedQuery {
    fn representative_program(&self) -> &QueryAccessProgramV1 {
        match self {
            Self::PresentOnly(exact) => exact.representative_program(),
            Self::Nullable(exact) => exact.representative_program(),
        }
    }

    fn value_parameters(&self) -> &[String] {
        match self {
            Self::PresentOnly(exact) => exact.value_parameters(),
            Self::Nullable(exact) => exact.value_parameters(),
        }
    }

    fn presence_parameters(&self) -> &[String] {
        match self {
            Self::PresentOnly(exact) => exact.presence_parameters(),
            Self::Nullable(exact) => exact.presence_parameters(),
        }
    }

    fn limit_parameter(&self) -> &str {
        match self {
            Self::PresentOnly(exact) => exact.limit_parameter(),
            Self::Nullable(exact) => exact.limit_parameter(),
        }
    }

    fn offset_parameter(&self) -> &str {
        match self {
            Self::PresentOnly(exact) => exact.offset_parameter(),
            Self::Nullable(exact) => exact.offset_parameter(),
        }
    }

    fn max_limit(&self) -> u16 {
        match self {
            Self::PresentOnly(exact) => exact.program().max_limit(),
            Self::Nullable(exact) => exact.program().max_limit(),
        }
    }

    fn max_offset(&self) -> u32 {
        match self {
            Self::PresentOnly(exact) => exact.program().max_offset(),
            Self::Nullable(exact) => exact.program().max_offset(),
        }
    }

    fn members(&self) -> &[riffdb_query_ir::ExactPredicateFamilyMemberV1] {
        match self {
            Self::PresentOnly(exact) => exact.program().members(),
            Self::Nullable(exact) => exact.program().members(),
        }
    }

    fn identity(&self) -> QueryPlanHash {
        match self {
            Self::PresentOnly(exact) => exact.identity(),
            Self::Nullable(exact) => exact.identity(),
        }
    }

    fn authorization_cost(&self) -> riffdb_types::QueryCostVectorV1 {
        match self {
            Self::PresentOnly(exact) => exact.authorization_cost(),
            Self::Nullable(exact) => exact.authorization_cost(),
        }
    }

    fn provider_descriptor_digest(
        &self,
    ) -> Result<riffdb_types::ProjectionProviderDescriptorHash, ()> {
        match self {
            Self::PresentOnly(exact) => exact
                .program()
                .provider_descriptor()
                .map(|value| value.digest()),
            Self::Nullable(exact) => exact
                .program()
                .provider_descriptor()
                .map(|value| value.digest()),
        }
        .map_err(|_| ())
    }

    #[allow(clippy::too_many_arguments)]
    fn provider_request(
        &self,
        partition_key: riffdb_types::PartitionKey,
        partition_value: CanonicalValue,
        policy_shape: riffdb_types::ApplicationRoleHash,
        row_policy: Option<Arc<AuthorizedQueryRowPolicyContextV1>>,
        parameters: BTreeMap<u16, ExactParameterValueV1>,
        member: riffdb_query_ir::ExactPredicateFamilyMemberV1,
        offset: u32,
        limit: NonZeroU16,
        minimum_epoch: Option<CommitSequence>,
    ) -> ExactPredicateProviderRequest {
        match self {
            Self::PresentOnly(exact) => {
                ExactPredicateProviderRequest::PresentOnly(ExactPredicateProjectionRequest::new(
                    Arc::clone(exact),
                    partition_key,
                    partition_value,
                    policy_shape,
                    row_policy,
                    parameters,
                    member,
                    offset,
                    limit,
                    minimum_epoch,
                ))
            }
            Self::Nullable(exact) => ExactPredicateProviderRequest::Nullable(
                NullableExactPredicateProjectionRequest::new(
                    Arc::clone(exact),
                    partition_key,
                    partition_value,
                    policy_shape,
                    row_policy,
                    parameters,
                    member,
                    offset,
                    limit,
                    minimum_epoch,
                ),
            ),
        }
    }
}

fn exact_parameter_value(value: &CanonicalValue) -> Option<ExactParameterValueV1> {
    match value {
        CanonicalValue::List(values) => ExactParameterValueV1::canonical_set(
            values
                .values()
                .iter()
                .map(exact_scalar_value)
                .collect::<Option<Vec<_>>>()?,
        )
        .ok(),
        _ => exact_scalar_value(value).map(ExactParameterValueV1::Scalar),
    }
}

fn exact_scalar_value(value: &CanonicalValue) -> Option<ExactScalarV1> {
    match value {
        CanonicalValue::Bool(value) => Some(ExactScalarV1::Bool(*value)),
        CanonicalValue::I64(value) => Some(ExactScalarV1::I64(*value)),
        CanonicalValue::U64(value) => Some(ExactScalarV1::U64(*value)),
        CanonicalValue::Decimal(value) => Some(ExactScalarV1::Decimal {
            coefficient: value.coefficient(),
            scale: value.spec().scale(),
        }),
        CanonicalValue::Money(value) => Some(ExactScalarV1::Money {
            currency: *value.currency().as_bytes(),
            coefficient: value.amount().coefficient(),
            scale: value.amount().spec().scale(),
        }),
        CanonicalValue::String(value) => Some(ExactScalarV1::String(value.as_str().to_owned())),
        CanonicalValue::Bytes(value) => Some(ExactScalarV1::Bytes(value.as_bytes().to_vec())),
        CanonicalValue::Timestamp(value) => i128::from(value.seconds())
            .checked_mul(1_000_000_000)
            .and_then(|seconds| seconds.checked_add(i128::from(value.nanoseconds())))
            .map(ExactScalarV1::Timestamp),
        CanonicalValue::Date(value) => Some(ExactScalarV1::Date(value.days_since_unix_epoch())),
        CanonicalValue::Uuid(value) => Some(ExactScalarV1::Uuid(*value)),
        CanonicalValue::Enum {
            type_id,
            variant_id,
        } => Some(ExactScalarV1::Enum {
            type_id: type_id.get(),
            variant_id: variant_id.get(),
        }),
        CanonicalValue::Null
        | CanonicalValue::List(_)
        | CanonicalValue::Record(_)
        | CanonicalValue::Vector(_) => None,
    }
}

#[allow(clippy::too_many_arguments)]
async fn execute_tokenized_named_query(
    service: Arc<RiffDbServiceInner>,
    context: RequestContext,
    bundle: riffdb_catalog::ValidatedContractBundle,
    tokenized: Arc<riffdb_query_module::CompiledTokenizedTextResultSetV1>,
    document: Arc<Document>,
    module_hash: QueryModuleHash,
    query_name: QueryOperationName,
    submitted: SymbolicQueryParameters,
    cursor: Option<CursorToken>,
    minimum_application_head: Option<u64>,
    consistency: Option<QueryConsistencyV1>,
) -> ServiceResult<ExecuteSymbolicQueryResult> {
    const OPERATION: ServiceOperationV1 = ServiceOperationV1::ExecuteQuery;
    let param_materialize_started = Instant::now();
    let program = tokenized.representative_program();
    let limit_name = document
        .body
        .bindings
        .first()
        .and_then(|binding| binding.take.as_ref())
        .and_then(|take| match &take.limit.value {
            riffdb_riffql_syntax::Expression::Parameter(name) => Some(name.value.as_str()),
            _ => None,
        })
        .ok_or_else(|| validation_failure(ValidationCode::InvalidValue))?;
    let offset_name = document
        .body
        .bindings
        .first()
        .and_then(|binding| binding.take.as_ref())
        .and_then(|take| take.offset.as_ref())
        .and_then(|offset| match &offset.value {
            riffdb_riffql_syntax::Expression::Parameter(name) => Some(name.value.as_str()),
            _ => None,
        })
        .ok_or_else(|| validation_failure(ValidationCode::InvalidValue))?;
    let submitted =
        exact_parameters_with_defaults(document.as_ref(), submitted, [limit_name, offset_name])
            .ok_or_else(|| validation_failure(ValidationCode::InvalidValue))?;
    let parameters = materialize_query_parameters(
        &service,
        OPERATION,
        bundle.bundle(),
        document.as_ref(),
        &submitted,
    )?;
    let target = application_query_target_with_identity(
        bundle.bundle(),
        program,
        &parameters,
        context.ingress(),
        tokenized.identity(),
        tokenized.authorization_cost(),
        Some(NonZeroU16::MIN),
    )
    .ok_or_else(|| validation_failure(ValidationCode::InvalidValue))?;
    let partition_value = parameters
        .get(program.partition_parameter())
        .cloned()
        .ok_or_else(|| service.internal_failure(OPERATION, InternalDefect::ProofMismatch))?;
    let query_text = match parameters.get(tokenized.query_parameter()) {
        Some(CanonicalValue::String(value)) => value.as_str().to_owned(),
        _ => return Err(validation_failure(ValidationCode::TypeMismatch)),
    };
    let mut limit = exact_u64_parameter(&document, &parameters, limit_name)
        .and_then(|value| u16::try_from(value).ok())
        .and_then(NonZeroU16::new)
        .ok_or_else(|| validation_failure(ValidationCode::InvalidValue))?;
    let mut offset = exact_u64_parameter(&document, &parameters, offset_name)
        .and_then(|value| u32::try_from(value).ok())
        .ok_or_else(|| validation_failure(ValidationCode::InvalidValue))?;
    let ranked =
        tokenized.tokenized_plan().ranking() == riffdb_query_ir::TokenizedRankingV1::RiffBm25V1;
    if !tokenized_cursor_request_allowed(ranked, cursor.is_some()) {
        return Err(application_validation_failure(
            ValidationCode::InvalidValue,
            ApplicationErrorCode::CursorInvalid,
        ));
    }
    let cursor_lookup = QueryCursorLookup::new(
        CursorContractIdentity::new(
            program.contract().lineage().clone(),
            program.contract().version(),
            program.contract().bundle_hash(),
        ),
        Some(module_hash),
        tokenized.identity(),
        query_cursor_parameter_hash(program, &parameters)
            .ok_or_else(|| validation_failure(ValidationCode::InvalidValue))?,
        context.principal().capability_id(),
        context.principal().capability_revision(),
    );
    let prior = match cursor {
        Some(token) => match service.cursors.resolve_tokenized_text(
            token,
            context.principal().principal_id(),
            &cursor_lookup,
        ) {
            Ok(state) => Some(state),
            Err(CursorAccessError::InvalidCursor) => {
                return Err(application_validation_failure(
                    ValidationCode::InvalidValue,
                    ApplicationErrorCode::CursorInvalid,
                ));
            }
            Err(CursorAccessError::Unavailable) => {
                return Err(PublicError::storage_unavailable().into());
            }
        },
        None => None,
    };
    if let Some(prior) = prior.as_deref() {
        if prior.result_ceiling() != tokenized.tokenized_plan().max_results()
            || minimum_application_head
                .is_some_and(|floor| floor > prior.minimum_application_head())
            || consistency.is_some_and(|value| value != QueryConsistencyV1::AdmissionHead)
        {
            return Err(application_validation_failure(
                ValidationCode::InvalidValue,
                ApplicationErrorCode::CursorInvalid,
            ));
        }
        offset = prior.next_offset();
        let remaining = tokenized
            .tokenized_plan()
            .max_results()
            .checked_sub(offset)
            .and_then(|remaining| u16::try_from(remaining).ok())
            .and_then(NonZeroU16::new)
            .ok_or_else(|| validation_failure(ValidationCode::InvalidValue))?;
        limit = NonZeroU16::new(limit.get().min(remaining.get()))
            .ok_or_else(|| validation_failure(ValidationCode::InvalidValue))?;
    }
    let (minimum_application_head, consistency) = match prior.as_deref() {
        Some(prior) => (Some(prior.minimum_application_head()), None),
        None if ranked => (
            minimum_application_head,
            Some(QueryConsistencyV1::AdmissionHead),
        ),
        None => (minimum_application_head, consistency),
    };
    if u32::from(limit.get()) > tokenized.tokenized_plan().max_results()
        || offset
            .checked_add(u32::from(limit.get()))
            .is_none_or(|end| end > tokenized.tokenized_plan().max_results())
    {
        return Err(validation_failure(ValidationCode::InvalidValue));
    }
    let operation_request = OperationRequest::execute_named_query(
        bundle.lineage().clone(),
        module_hash,
        query_name,
        target,
    )
    .map_err(|_| service.internal_failure(OPERATION, InternalDefect::ProofMismatch))?;
    record_read_stage(
        &service,
        ReadPipelineStage::ParamMaterialize,
        param_materialize_started,
    );
    let authorize_begin_started = Instant::now();
    let begun = begin_symbolic(&service, &context, &bundle, operation_request, OPERATION).await?;
    record_read_stage(
        &service,
        ReadPipelineStage::AuthorizeBegin,
        authorize_begin_started,
    );
    let authorize_pre_started = Instant::now();
    let execution_authorization = begun
        .reauthorize_read(&service, &context)
        .await?
        .into_application_query()
        .ok_or_else(|| service.internal_failure(OPERATION, InternalDefect::ProofMismatch))?;
    let minimum_application_head = match resolve_admission_head_floor(
        &service,
        &context,
        minimum_application_head,
        consistency,
    )
    .await
    {
        Ok(floor) => floor,
        Err(failure) => return Err(finish_failure(&service, &context, &begun, failure).await),
    };
    let minimum_epoch = minimum_application_head
        .map(|value| {
            CommitSequence::new(value)
                .ok_or_else(|| validation_failure(ValidationCode::InvalidValue))
        })
        .transpose()?;
    let row_policy =
        resolve_authorized_query_row_policy_context(&execution_authorization, bundle.bundle())
            .map_err(|_| service.internal_failure(OPERATION, InternalDefect::ProofMismatch))?
            .map(Arc::new);
    let row_policy_identity =
        match row_policy.as_deref() {
            Some(policy) => Some(policy.internal_capability_identity().ok_or_else(|| {
                service.internal_failure(OPERATION, InternalDefect::ProofMismatch)
            })?),
            None => None,
        };
    let Some(policy_shape) = execution_authorization.application_role_hash() else {
        let failure = application_validation_failure(
            ValidationCode::InvalidValue,
            ApplicationErrorCode::QueryUnavailable,
        );
        return Err(finish_failure(&service, &context, &begun, failure).await);
    };
    let request = TokenizedTextProjectionRequest::new(
        Arc::clone(&tokenized),
        execution_authorization
            .target()
            .partition()
            .ok_or_else(|| service.internal_failure(OPERATION, InternalDefect::ProofMismatch))?
            .partition_key()
            .clone(),
        partition_value,
        policy_shape,
        row_policy,
        query_text,
        offset,
        limit,
        minimum_epoch,
        prior
            .as_deref()
            .map(|prior| (prior.epoch(), prior.generation())),
    );
    let Some(provider) = service.providers.tokenized_text.as_ref() else {
        let failure = PublicError::storage_unavailable().into();
        return Err(finish_failure(&service, &context, &begun, failure).await);
    };
    record_read_stage(
        &service,
        ReadPipelineStage::AuthorizePre,
        authorize_pre_started,
    );
    let execute_started = Instant::now();
    let observed = match execute_exact_provider_with_readiness(&service, &context, &begun, || {
        provider.execute(request.clone())
    })
    .await?
    {
        Ok(observed) => observed,
        Err(error) => {
            let Some(code) = exact_projection_application_code(error) else {
                let failure = service.internal_failure(OPERATION, InternalDefect::ProofMismatch);
                return Err(finish_failure(&service, &context, &begun, failure).await);
            };
            let failure = application_validation_failure(ValidationCode::InvalidValue, code);
            return Err(finish_failure(&service, &context, &begun, failure).await);
        }
    };
    record_read_stage(&service, ReadPipelineStage::Execute, execute_started);
    if observed.provider() != tokenized.tokenized_plan().descriptor().digest()
        || minimum_epoch.is_some_and(|minimum| observed.epoch() < minimum)
    {
        let failure = service.internal_failure(OPERATION, InternalDefect::ProofMismatch);
        return Err(finish_failure(&service, &context, &begun, failure).await);
    }
    let statistics_identity = observed
        .statistics_identity()
        .ok_or_else(|| service.internal_failure(OPERATION, InternalDefect::ProofMismatch))?;
    if prior.as_deref().is_some_and(|prior| {
        prior.epoch() != observed.epoch()
            || prior.generation() != observed.generation()
            || prior.history_incarnation() != observed.history_incarnation()
            || prior.statistics_identity() != statistics_identity
    }) {
        let failure = service.internal_failure(OPERATION, InternalDefect::ProofMismatch);
        return Err(finish_failure(&service, &context, &begun, failure).await);
    }
    let returned = u32::try_from(observed.rows().len())
        .map_err(|_| service.internal_failure(OPERATION, InternalDefect::ProofMismatch))?;
    let next_offset = offset
        .checked_add(returned)
        .ok_or_else(|| service.internal_failure(OPERATION, InternalDefect::ProofMismatch))?;
    let has_more = u64::from(next_offset) < observed.exact_total()
        && next_offset < tokenized.tokenized_plan().max_results();
    let cursor_guard = if ranked && has_more {
        let floor = minimum_application_head
            .ok_or_else(|| service.internal_failure(OPERATION, InternalDefect::ProofMismatch))?;
        match service.cursors.register_tokenized_text_unpublished(
            context.principal().principal_id(),
            cursor_lookup,
            TokenizedTextCursorState::new(
                next_offset,
                observed.epoch(),
                observed.generation(),
                floor,
                observed.history_incarnation(),
                statistics_identity,
                tokenized.tokenized_plan().max_results(),
            ),
        ) {
            Ok(guard) => {
                if guard.capacity_evicted() {
                    service
                        .providers
                        .telemetry
                        .record(ServiceTelemetryEvent::CursorEvicted);
                }
                Some(guard)
            }
            Err(_) => {
                let failure = PublicError::storage_unavailable().into();
                return Err(finish_failure(&service, &context, &begun, failure).await);
            }
        }
    } else {
        None
    };
    let authorize_post_started = Instant::now();
    let release_authorization = begun
        .reauthorize_read(&service, &context)
        .await?
        .into_application_query()
        .ok_or_else(|| service.internal_failure(OPERATION, InternalDefect::ProofMismatch))?;
    let release_row_policy =
        resolve_authorized_query_row_policy_context(&release_authorization, bundle.bundle())
            .map_err(|_| service.internal_failure(OPERATION, InternalDefect::ProofMismatch))?;
    let release_row_policy_identity =
        match release_row_policy.as_ref() {
            Some(policy) => Some(policy.internal_capability_identity().ok_or_else(|| {
                service.internal_failure(OPERATION, InternalDefect::ProofMismatch)
            })?),
            None => None,
        };
    if release_authorization.application_role_hash() != Some(policy_shape)
        || release_row_policy_identity != row_policy_identity
    {
        let failure = application_validation_failure(
            ValidationCode::InvalidValue,
            ApplicationErrorCode::QueryUnavailable,
        );
        return Err(finish_failure(&service, &context, &begun, failure).await);
    }
    record_read_stage(
        &service,
        ReadPipelineStage::AuthorizePost,
        authorize_post_started,
    );
    let response_build_started = Instant::now();
    let mut result = tokenized_result_response(
        &tokenized,
        &document,
        module_hash,
        observed,
        Arc::clone(bundle.enum_variant_names()),
    )
    .ok_or_else(|| service.internal_failure(OPERATION, InternalDefect::ProofMismatch))?;
    record_read_stage(
        &service,
        ReadPipelineStage::ResponseBuild,
        response_build_started,
    );
    let audit_finish_started = Instant::now();
    finish_success(&service, &context, &begun).await?;
    result.next_cursor = cursor_guard.map(crate::CursorPublicationGuard::publish);
    record_read_stage(
        &service,
        ReadPipelineStage::AuditFinish,
        audit_finish_started,
    );
    Ok(result)
}

const fn tokenized_cursor_request_allowed(ranked: bool, cursor_present: bool) -> bool {
    !cursor_present || ranked
}

#[cfg(test)]
mod tokenized_cursor_request_tests {
    use super::tokenized_cursor_request_allowed;

    #[test]
    fn ranked_continuations_are_reachable_and_boolean_cursors_are_refused() {
        assert!(tokenized_cursor_request_allowed(false, false));
        assert!(tokenized_cursor_request_allowed(true, false));
        assert!(tokenized_cursor_request_allowed(true, true));
        assert!(!tokenized_cursor_request_allowed(false, true));
    }
}

#[allow(clippy::too_many_arguments)]
async fn execute_exact_named_query(
    service: Arc<RiffDbServiceInner>,
    context: RequestContext,
    bundle: riffdb_catalog::ValidatedContractBundle,
    exact: Arc<riffdb_query_module::CompiledExactTextResultSetV1>,
    document: Arc<Document>,
    module_hash: QueryModuleHash,
    query_name: QueryOperationName,
    submitted: SymbolicQueryParameters,
    cursor: Option<CursorToken>,
    minimum_application_head: Option<u64>,
    consistency: Option<QueryConsistencyV1>,
) -> ServiceResult<ExecuteSymbolicQueryResult> {
    const OPERATION: ServiceOperationV1 = ServiceOperationV1::ExecuteQuery;
    if cursor.is_some() {
        return Err(application_validation_failure(
            ValidationCode::InvalidValue,
            ApplicationErrorCode::CursorInvalid,
        ));
    }
    let param_materialize_started = Instant::now();
    let submitted = exact_parameters_with_defaults(
        document.as_ref(),
        submitted,
        [exact.limit_parameter(), exact.offset_parameter()],
    )
    .ok_or_else(|| validation_failure(ValidationCode::InvalidValue))?;
    let program = exact.representative_program();
    let parameters = materialize_query_parameters(
        &service,
        OPERATION,
        bundle.bundle(),
        document.as_ref(),
        &submitted,
    )?;
    let target = application_query_target_with_identity(
        bundle.bundle(),
        program,
        &parameters,
        context.ingress(),
        exact.identity(),
        exact.authorization_cost(),
        Some(NonZeroU16::MIN),
    )
    .ok_or_else(|| validation_failure(ValidationCode::InvalidValue))?;
    let partition_value = parameters
        .get(program.partition_parameter())
        .cloned()
        .ok_or_else(|| service.internal_failure(OPERATION, InternalDefect::ProofMismatch))?;
    let needle = match parameters.get(exact.needle_parameter()) {
        Some(CanonicalValue::String(value)) => ExactTextProfileV1::BinaryUtf8V1
            .bind_needle(value.as_str())
            .map_err(|_| validation_failure(ValidationCode::InvalidValue))?,
        _ => return Err(validation_failure(ValidationCode::TypeMismatch)),
    };
    let filter_value = exact
        .filter()
        .and_then(|filter| parameters.get(filter.parameter()))
        .cloned();
    let limit = exact_u64_parameter(&document, &parameters, exact.limit_parameter())
        .and_then(|value| u16::try_from(value).ok())
        .and_then(NonZeroU16::new)
        .ok_or_else(|| validation_failure(ValidationCode::InvalidValue))?;
    let offset = exact_u64_parameter(&document, &parameters, exact.offset_parameter())
        .and_then(|value| u32::try_from(value).ok())
        .ok_or_else(|| validation_failure(ValidationCode::InvalidValue))?;
    exact
        .binding()
        .plan()
        .bind_window(offset, limit)
        .map_err(|_| validation_failure(ValidationCode::InvalidValue))?;
    let operation_request = OperationRequest::execute_named_query(
        bundle.lineage().clone(),
        module_hash,
        query_name,
        target,
    )
    .map_err(|_| service.internal_failure(OPERATION, InternalDefect::ProofMismatch))?;
    record_read_stage(
        &service,
        ReadPipelineStage::ParamMaterialize,
        param_materialize_started,
    );
    let authorize_begin_started = Instant::now();
    let begun = begin_symbolic(&service, &context, &bundle, operation_request, OPERATION).await?;
    record_read_stage(
        &service,
        ReadPipelineStage::AuthorizeBegin,
        authorize_begin_started,
    );

    let authorize_pre_started = Instant::now();
    let execution_authorization = begun
        .reauthorize_read(&service, &context)
        .await?
        .into_application_query()
        .ok_or_else(|| service.internal_failure(OPERATION, InternalDefect::ProofMismatch))?;
    let minimum_application_head = match resolve_admission_head_floor(
        &service,
        &context,
        minimum_application_head,
        consistency,
    )
    .await
    {
        Ok(floor) => floor,
        Err(failure) => return Err(finish_failure(&service, &context, &begun, failure).await),
    };
    let minimum_epoch = minimum_application_head
        .map(|value| {
            CommitSequence::new(value)
                .ok_or_else(|| validation_failure(ValidationCode::InvalidValue))
        })
        .transpose()?;
    // Resolve the exact current row-policy authority before provider selection.
    // The provider applies its opaque candidate-bound proof before it builds
    // count, rank, or ordinal state; public execution never post-filters.
    let row_policy =
        resolve_authorized_query_row_policy_context(&execution_authorization, bundle.bundle())
            .map_err(|_| service.internal_failure(OPERATION, InternalDefect::ProofMismatch))?
            .map(Arc::new);
    let row_policy_identity =
        match row_policy.as_deref() {
            Some(policy) => Some(policy.internal_capability_identity().ok_or_else(|| {
                service.internal_failure(OPERATION, InternalDefect::ProofMismatch)
            })?),
            None => None,
        };
    let Some(policy_shape) = execution_authorization.application_role_hash() else {
        let failure = application_validation_failure(
            ValidationCode::InvalidValue,
            ApplicationErrorCode::QueryUnavailable,
        );
        return Err(finish_failure(&service, &context, &begun, failure).await);
    };
    let request = ExactTextProjectionRequest::new(
        Arc::clone(&exact),
        execution_authorization
            .target()
            .partition()
            .ok_or_else(|| service.internal_failure(OPERATION, InternalDefect::ProofMismatch))?
            .partition_key()
            .clone(),
        partition_value,
        policy_shape,
        row_policy,
        needle,
        filter_value,
        offset,
        limit,
        minimum_epoch,
    );
    let Some(provider) = service.providers.exact_text.as_ref() else {
        let failure = PublicError::storage_unavailable().into();
        return Err(finish_failure(&service, &context, &begun, failure).await);
    };
    record_read_stage(
        &service,
        ReadPipelineStage::AuthorizePre,
        authorize_pre_started,
    );
    let execute_started = Instant::now();
    let observed = match execute_exact_provider_with_readiness(&service, &context, &begun, || {
        provider.execute(request.clone())
    })
    .await?
    {
        Ok(observed) => observed,
        Err(error) => {
            let Some(code) = exact_projection_application_code(error) else {
                let failure = service.internal_failure(OPERATION, InternalDefect::ProofMismatch);
                return Err(finish_failure(&service, &context, &begun, failure).await);
            };
            let failure = application_validation_failure(ValidationCode::InvalidValue, code);
            return Err(finish_failure(&service, &context, &begun, failure).await);
        }
    };
    record_read_stage(&service, ReadPipelineStage::Execute, execute_started);
    if observed.provider() != exact.binding().plan().provider_digest()
        || minimum_epoch.is_some_and(|minimum| observed.epoch() < minimum)
    {
        let failure = service.internal_failure(OPERATION, InternalDefect::ProofMismatch);
        return Err(finish_failure(&service, &context, &begun, failure).await);
    }
    let authorize_post_started = Instant::now();
    // Fresh authorization safe point 3 occurs after the complete page/count
    // epoch is selected and before any value crosses the service boundary.
    let release_authorization = begun
        .reauthorize_read(&service, &context)
        .await?
        .into_application_query()
        .ok_or_else(|| service.internal_failure(OPERATION, InternalDefect::ProofMismatch))?;
    let release_row_policy =
        resolve_authorized_query_row_policy_context(&release_authorization, bundle.bundle())
            .map_err(|_| service.internal_failure(OPERATION, InternalDefect::ProofMismatch))?;
    let release_row_policy_identity =
        match release_row_policy.as_ref() {
            Some(policy) => Some(policy.internal_capability_identity().ok_or_else(|| {
                service.internal_failure(OPERATION, InternalDefect::ProofMismatch)
            })?),
            None => None,
        };
    if release_authorization.application_role_hash() != Some(policy_shape)
        || release_row_policy_identity != row_policy_identity
    {
        let failure = application_validation_failure(
            ValidationCode::InvalidValue,
            ApplicationErrorCode::QueryUnavailable,
        );
        return Err(finish_failure(&service, &context, &begun, failure).await);
    }
    record_read_stage(
        &service,
        ReadPipelineStage::AuthorizePost,
        authorize_post_started,
    );
    let response_build_started = Instant::now();
    let result = exact_result_response(
        &exact,
        &document,
        module_hash,
        observed,
        Arc::clone(bundle.enum_variant_names()),
    )
    .ok_or_else(|| service.internal_failure(OPERATION, InternalDefect::ProofMismatch))?;
    record_read_stage(
        &service,
        ReadPipelineStage::ResponseBuild,
        response_build_started,
    );
    let audit_finish_started = Instant::now();
    finish_success(&service, &context, &begun).await?;
    record_read_stage(
        &service,
        ReadPipelineStage::AuditFinish,
        audit_finish_started,
    );
    Ok(result)
}

async fn execute_provider_with_readiness<T, E, Execute, Classify, Reauthorize, ReauthorizeFuture>(
    control: &crate::RequestControl,
    deadline_scheduler: &dyn crate::RequestDeadlineScheduler,
    mut execute: Execute,
    classify: Classify,
    mut reauthorize: Reauthorize,
) -> ServiceResult<Result<T, E>>
where
    Execute: FnMut() -> Result<T, E>,
    Classify: Fn(&E) -> Option<ExactTextProjectionPortError>,
    Reauthorize: FnMut() -> ReauthorizeFuture,
    ReauthorizeFuture: std::future::Future<Output = ServiceResult<()>>,
{
    let readiness_deadline = Instant::now()
        .checked_add(EXACT_PROVIDER_READINESS_WAIT)
        .unwrap_or_else(|| control.deadline())
        .min(control.deadline());
    let mut observations = 0_u16;
    loop {
        observations = observations.saturating_add(1);
        match execute() {
            Ok(result) => return Ok(Ok(result)),
            Err(error)
                if classify(&error).is_some_and(exact_provider_readiness_pending)
                    && observations < MAX_EXACT_PROVIDER_READINESS_OBSERVATIONS
                    && Instant::now() < readiness_deadline =>
            {
                let wake_at = Instant::now()
                    .checked_add(EXACT_PROVIDER_READINESS_POLL)
                    .unwrap_or(readiness_deadline)
                    .min(readiness_deadline);
                wait_with_control(
                    control,
                    deadline_scheduler,
                    deadline_scheduler.wait_until(wake_at),
                )
                .await
                .map_err(controlled_failure)?;
                reauthorize().await?;
            }
            Err(error) => return Ok(Err(error)),
        }
    }
}

const fn exact_provider_readiness_pending(error: ExactTextProjectionPortError) -> bool {
    matches!(
        error,
        ExactTextProjectionPortError::Building
            | ExactTextProjectionPortError::Rebuilding
            | ExactTextProjectionPortError::FreshnessUnsatisfied
    )
}

async fn execute_exact_provider_with_readiness<T>(
    service: &RiffDbServiceInner,
    context: &RequestContext,
    begun: &crate::orchestration::BegunInvocation,
    execute: impl FnMut() -> Result<T, ExactTextProjectionPortError>,
) -> ServiceResult<Result<T, ExactTextProjectionPortError>> {
    execute_provider_with_readiness(
        context.control(),
        service.providers.deadline_scheduler.as_ref(),
        execute,
        |error| Some(*error),
        || async {
            begun.reauthorize_read(service, context).await?;
            Ok(())
        },
    )
    .await
}

const fn exact_projection_application_code(
    error: ExactTextProjectionPortError,
) -> Option<ApplicationErrorCode> {
    match error {
        ExactTextProjectionPortError::InputInvalid => Some(ApplicationErrorCode::InputInvalid),
        ExactTextProjectionPortError::ResponseTooLarge => {
            Some(ApplicationErrorCode::ResponseTooLarge)
        }
        ExactTextProjectionPortError::SnapshotRetired => {
            Some(ApplicationErrorCode::SnapshotRetired)
        }
        ExactTextProjectionPortError::FreshnessUnsatisfied => {
            Some(ApplicationErrorCode::FreshnessUnsatisfied)
        }
        ExactTextProjectionPortError::Diverged => Some(ApplicationErrorCode::ProjectionDiverged),
        ExactTextProjectionPortError::Building
        | ExactTextProjectionPortError::Rebuilding
        | ExactTextProjectionPortError::Unavailable => Some(ApplicationErrorCode::QueryUnavailable),
        ExactTextProjectionPortError::Integrity => None,
    }
}

fn exact_u64_parameter(
    document: &Document,
    parameters: &QueryParameters,
    name: &str,
) -> Option<u64> {
    if let Some(CanonicalValue::U64(value)) = parameters.get(name) {
        return Some(*value);
    }
    let default = document
        .parameters
        .iter()
        .find(|parameter| parameter.name.value.as_str() == name)?
        .default
        .as_ref()?;
    match &default.value {
        Literal::Unsigned(value) => value.parse().ok(),
        Literal::String(_) | Literal::Boolean(_) | Literal::Null => None,
    }
}

fn exact_parameters_with_defaults(
    document: &Document,
    mut submitted: SymbolicQueryParameters,
    names: [&str; 2],
) -> Option<SymbolicQueryParameters> {
    for name in names {
        if submitted.0.contains_key(name) {
            continue;
        }
        let parameter = document
            .parameters
            .iter()
            .find(|parameter| parameter.name.value.as_str() == name)?;
        let Literal::Unsigned(value) = &parameter.default.as_ref()?.value else {
            return None;
        };
        submitted
            .0
            .insert(name.to_owned(), SubmittedValue::U64(value.parse().ok()?));
    }
    Some(submitted)
}

fn tokenized_result_response(
    tokenized: &riffdb_query_module::CompiledTokenizedTextResultSetV1,
    document: &Document,
    module_hash: QueryModuleHash,
    observed: ExactTextProjectionResult,
    enum_variant_names: SharedEnumVariantNames,
) -> Option<ExecuteSymbolicQueryResult> {
    let program = tokenized.representative_program();
    let step = program.steps().first()?;
    if program.steps().len() != 1 || step.result_names().len() != 1 {
        return None;
    }
    let access = program.internal_entity_access(step.entity())?;
    let field_names = step
        .selected_fields()
        .iter()
        .map(|name| access.internal_field_id(name).map(|field| (field, name)))
        .collect::<Option<BTreeMap<FieldId, &String>>>()?;
    let (rows, _exact_total, epoch, _generation, _provider, _history_incarnation) =
        observed.into_parts();
    let rows = rows
        .into_iter()
        .map(|row| {
            let (_key, output) = row.into_parts();
            let fields = output
                .into_fields()
                .into_iter()
                .map(|(field, value)| {
                    field_names
                        .get(&field)
                        .map(|name| (Arc::<str>::from(name.as_str()), value))
                })
                .collect::<Option<BTreeMap<_, _>>>()?;
            (fields.len() == field_names.len()).then_some(SymbolicResultRecord {
                entity: Arc::from(step.entity()),
                fields,
                nested: BTreeMap::new(),
                exact_decimals: BTreeMap::new(),
                exact_means: BTreeMap::new(),
            })
        })
        .collect::<Option<Vec<_>>>()?;
    let row_result_name = selection_result_name(
        &document.body.selection,
        &document.body.bindings.first()?.name.value,
    )?;
    let fields = BTreeMap::from([(row_result_name, SymbolicResultField::Many(rows))]);
    Some(ExecuteSymbolicQueryResult {
        identity: SymbolicQueryIdentity::from_named_plan(
            program,
            module_hash,
            tokenized.identity(),
        ),
        outcome: document.body.outcome.as_ref()?.value.as_str().to_owned(),
        application_head: epoch.get(),
        fields,
        compact_result: None,
        packed_result: false,
        enum_variant_names,
        next_cursor: None,
    })
}

fn exact_result_response(
    exact: &riffdb_query_module::CompiledExactTextResultSetV1,
    document: &Document,
    module_hash: QueryModuleHash,
    observed: ExactTextProjectionResult,
    enum_variant_names: SharedEnumVariantNames,
) -> Option<ExecuteSymbolicQueryResult> {
    let program = exact.representative_program();
    let step = program.steps().first()?;
    if program.steps().len() != 1 || step.result_names().len() != 1 {
        return None;
    }
    let access = program.internal_entity_access(step.entity())?;
    let field_names = step
        .selected_fields()
        .iter()
        .map(|name| access.internal_field_id(name).map(|field| (field, name)))
        .collect::<Option<BTreeMap<FieldId, &String>>>()?;
    let (rows, exact_total, epoch, _generation, _provider, _history_incarnation) =
        observed.into_parts();
    let rows = rows
        .into_iter()
        .map(|row| {
            let (_key, output) = row.into_parts();
            let fields = output
                .into_fields()
                .into_iter()
                .map(|(field, value)| {
                    field_names
                        .get(&field)
                        .map(|name| (Arc::<str>::from(name.as_str()), value))
                })
                .collect::<Option<BTreeMap<_, _>>>()?;
            (fields.len() == field_names.len()).then_some(SymbolicResultRecord {
                entity: Arc::from(step.entity()),
                fields,
                nested: BTreeMap::new(),
                exact_decimals: BTreeMap::new(),
                exact_means: BTreeMap::new(),
            })
        })
        .collect::<Option<Vec<_>>>()?;

    let aggregate = document.body.aggregates.first()?;
    if document.body.aggregates.len() != 1
        || aggregate.measures.len() != 1
        || aggregate.source.value.as_str() != step.binding()
    {
        return None;
    }
    let measure = aggregate.measures.first()?;
    if measure.function.value != riffdb_riffql_syntax::AggregateFunction::ExactCount {
        return None;
    }
    let aggregate_result_name =
        selection_result_name(&document.body.selection, &aggregate.name.value)?;
    let row_result_name = selection_result_name(
        &document.body.selection,
        &document.body.bindings.first()?.name.value,
    )?;
    let mut fields = BTreeMap::new();
    fields.insert(row_result_name, SymbolicResultField::Many(rows));
    fields.insert(
        aggregate_result_name,
        SymbolicResultField::One(SymbolicResultRecord {
            entity: Arc::from(aggregate.name.value.as_str()),
            fields: BTreeMap::from([(
                Arc::from(measure.alias.value.as_str()),
                CanonicalValue::U64(exact_total),
            )]),
            nested: BTreeMap::new(),
            exact_decimals: BTreeMap::new(),
            exact_means: BTreeMap::new(),
        }),
    );
    Some(ExecuteSymbolicQueryResult {
        identity: SymbolicQueryIdentity::from_named_plan(program, module_hash, exact.identity()),
        outcome: document.body.outcome.as_ref()?.value.as_str().to_owned(),
        application_head: epoch.get(),
        fields,
        compact_result: None,
        packed_result: false,
        enum_variant_names,
        next_cursor: None,
    })
}

fn exact_predicate_result_response(
    exact: &ExactPredicateNamedQuery,
    document: &Document,
    module_hash: QueryModuleHash,
    observed: ExactPredicateProjectionResult,
    enum_variant_names: SharedEnumVariantNames,
) -> Option<ExecuteSymbolicQueryResult> {
    let access_program = exact.representative_program();
    let step = access_program.steps().first()?;
    if access_program.steps().len() != 1 || step.result_names().len() != 1 {
        return None;
    }
    let access = access_program.internal_entity_access(step.entity())?;
    let field_names = step
        .selected_fields()
        .iter()
        .map(|name| access.internal_field_id(name).map(|field| (field, name)))
        .collect::<Option<BTreeMap<FieldId, &String>>>()?;
    let (rows, exact_total, epoch, _generation, _provider, _history_incarnation) =
        observed.into_parts();
    let rows = rows
        .into_iter()
        .map(|row| {
            let (_key, output) = row.into_parts();
            let fields = output
                .into_fields()
                .into_iter()
                .map(|(field, value)| {
                    field_names
                        .get(&field)
                        .map(|name| (Arc::<str>::from(name.as_str()), value))
                })
                .collect::<Option<BTreeMap<_, _>>>()?;
            (fields.len() == field_names.len()).then_some(SymbolicResultRecord {
                entity: Arc::from(step.entity()),
                fields,
                nested: BTreeMap::new(),
                exact_decimals: BTreeMap::new(),
                exact_means: BTreeMap::new(),
            })
        })
        .collect::<Option<Vec<_>>>()?;
    let aggregate = document.body.aggregates.first()?;
    if document.body.aggregates.len() != 1
        || aggregate.measures.len() != 1
        || aggregate.source.value.as_str() != step.binding()
    {
        return None;
    }
    let measure = aggregate.measures.first()?;
    if measure.function.value != riffdb_riffql_syntax::AggregateFunction::ExactCount {
        return None;
    }
    let aggregate_result_name =
        selection_result_name(&document.body.selection, &aggregate.name.value)?;
    let row_result_name = selection_result_name(
        &document.body.selection,
        &document.body.bindings.first()?.name.value,
    )?;
    let mut fields = BTreeMap::new();
    fields.insert(row_result_name, SymbolicResultField::Many(rows));
    fields.insert(
        aggregate_result_name,
        SymbolicResultField::One(SymbolicResultRecord {
            entity: Arc::from(aggregate.name.value.as_str()),
            fields: BTreeMap::from([(
                Arc::from(measure.alias.value.as_str()),
                CanonicalValue::U64(exact_total),
            )]),
            nested: BTreeMap::new(),
            exact_decimals: BTreeMap::new(),
            exact_means: BTreeMap::new(),
        }),
    );
    Some(ExecuteSymbolicQueryResult {
        identity: SymbolicQueryIdentity::from_named_plan(
            access_program,
            module_hash,
            exact.identity(),
        ),
        outcome: document.body.outcome.as_ref()?.value.as_str().to_owned(),
        application_head: epoch.get(),
        fields,
        compact_result: None,
        packed_result: false,
        enum_variant_names,
        next_cursor: None,
    })
}

fn selection_result_name(
    selection: &riffdb_riffql_syntax::Selection,
    source: &riffdb_riffql_syntax::Identifier,
) -> Option<String> {
    let field = selection.fields.iter().find(|field| {
        field
            .source
            .value
            .0
            .first()
            .is_some_and(|segment| segment.value.as_str() == source.as_str())
    })?;
    Some(
        field
            .alias
            .as_ref()
            .map_or(source.as_str(), |alias| alias.value.as_str())
            .to_owned(),
    )
}

/// Executes one compiler-owned portability observation under current V7 authority.
///
/// No cursor or application result escapes this boundary. Only a deterministic
/// semantic digest is returned to the reimport coordinator.
pub(crate) async fn execute_reimport_observation(
    service: &Arc<RiffDbServiceInner>,
    context: &RequestContext,
    manifest: &riffdb_application::ApplicationPortabilityManifest,
    observation: &ReimportObservation,
    authorization: Box<AuthorizedApplicationReimportV1>,
) -> ServiceResult<ReimportObservationResult> {
    const OPERATION: ServiceOperationV1 = ServiceOperationV1::ApplyApplicationReimportPage;
    let catalog = wait_with_control(
        context.control(),
        service.providers.deadline_scheduler.as_ref(),
        service
            .providers
            .catalog
            .prepare_active_catalog(context.control()),
    )
    .await
    .map_err(controlled_failure)?
    .map_err(|_| service.internal_failure(OPERATION, InternalDefect::ProofMismatch))?
    .ok_or_else(|| service.internal_failure(OPERATION, InternalDefect::ProofMismatch))?;
    let bundle = catalog.bundle();
    manifest
        .validate_compiled_contract(bundle.bundle())
        .map_err(|_| service.internal_failure(OPERATION, InternalDefect::ProofMismatch))?;
    let module_hash = observation
        .module_hash()
        .ok_or_else(|| service.internal_failure(OPERATION, InternalDefect::ProofMismatch))?;
    let module = load_query_module(
        service,
        context,
        bundle.clone(),
        Some(module_hash),
        OPERATION,
    )
    .await?
    .ok_or_else(|| service.internal_failure(OPERATION, InternalDefect::ProofMismatch))?;
    let query = module
        .module()
        .query(observation.query().as_str())
        .ok_or_else(|| service.internal_failure(OPERATION, InternalDefect::ProofMismatch))?;
    let submitted = SymbolicQueryParameters::new(
        observation
            .parameters()
            .iter()
            .map(|parameter| {
                let value = parameter.value().map_err(|_| {
                    service.internal_failure(OPERATION, InternalDefect::ProofMismatch)
                })?;
                let value = SubmittedValue::try_from(value).map_err(|_| {
                    service.internal_failure(OPERATION, InternalDefect::ProofMismatch)
                })?;
                Ok((parameter.name().as_str().to_owned(), value))
            })
            .collect::<ServiceResult<BTreeMap<_, _>>>()?,
    )
    .map_err(|_| service.internal_failure(OPERATION, InternalDefect::ProofMismatch))?;
    let presence = query
        .operational_family()
        .map(|family| {
            family
                .presence_parameters()
                .iter()
                .map(|name| {
                    submitted
                        .get(name)
                        .is_some_and(|value| !matches!(value, SubmittedValue::Null))
                })
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    let program = query
        .select_program(&presence)
        .ok_or_else(|| service.internal_failure(OPERATION, InternalDefect::ProofMismatch))?;
    let aggregates = query
        .operational_family()
        .map_or_else(|| Arc::from([]), |family| Arc::from(family.aggregates()));
    let parameters = materialize_query_parameters(
        service,
        OPERATION,
        bundle.bundle(),
        query.shared_document().as_ref(),
        &submitted,
    )?;
    let target =
        application_query_target(bundle.bundle(), &program, &parameters, context.ingress())
            .ok_or_else(|| service.internal_failure(OPERATION, InternalDefect::ProofMismatch))?;
    let authorization = authorization
        .into_query_execution(target)
        .map_err(|_| service.internal_failure(OPERATION, InternalDefect::ProofMismatch))?;
    let row_policy =
        resolve_authorized_query_row_policy_context(&authorization, bundle.bundle())
            .map_err(|_| service.internal_failure(OPERATION, InternalDefect::ProofMismatch))?;
    let executor = service
        .providers
        .query_executor
        .as_ref()
        .ok_or_else(PublicError::storage_unavailable)?;
    let snapshot = execute_authorized_query_page(
        &authorization,
        executor.as_ref(),
        &program,
        &aggregates,
        &parameters,
        None,
        row_policy.as_ref(),
    )
    .map_err(|error| execution_failure(service, OPERATION, error))?;
    if snapshot.continuation().is_some() {
        return Err(service.internal_failure(OPERATION, InternalDefect::ProofMismatch));
    }
    let items = query_snapshot_item_count(&snapshot)
        .ok_or_else(|| service.internal_failure(OPERATION, InternalDefect::ProofMismatch))?;
    if items > u64::from(observation.maximum_items()) {
        return Err(service.internal_failure(OPERATION, InternalDefect::ProofMismatch));
    }
    let result = ExecuteSymbolicQueryResult::from_named_snapshot(
        &program,
        module_hash,
        snapshot,
        Arc::clone(bundle.enum_variant_names()),
    );
    let actual = hash_reimport_query_result(&result)
        .ok_or_else(|| service.internal_failure(OPERATION, InternalDefect::ProofMismatch))?;
    Ok(ReimportObservationResult::new(
        observation.name().clone(),
        actual,
    ))
}

fn query_snapshot_item_count(snapshot: &QueryOwnedSnapshot) -> Option<u64> {
    let named = snapshot.fields().values().try_fold(0_u64, |total, field| {
        let count = match field {
            QueryResultValue::One(_) | QueryResultValue::AggregateOne(_) => 1,
            QueryResultValue::Maybe(value) => u64::from(value.is_some()),
            QueryResultValue::Many(values) => u64::try_from(values.len()).ok()?,
            QueryResultValue::AggregateMany(values) => u64::try_from(values.len()).ok()?,
        };
        total.checked_add(count)
    })?;
    snapshot.covered_result().map_or(Some(named), |covered| {
        named.checked_add(u64::try_from(covered.rows().len()).ok()?)
    })
}

fn hash_reimport_query_result(
    result: &ExecuteSymbolicQueryResult,
) -> Option<riffdb_types::GeneratedArtifactHash> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(b"riffdb.reimport-observation/v1\0");
    push_observation_bytes(&mut bytes, result.outcome().as_bytes())?;
    bytes.extend_from_slice(&u32::try_from(result.fields().len()).ok()?.to_be_bytes());
    for (name, value) in result.fields() {
        push_observation_bytes(&mut bytes, name.as_bytes())?;
        match value {
            SymbolicResultField::One(row) => {
                bytes.push(1);
                hash_observation_row(&mut bytes, row)?;
            }
            SymbolicResultField::Maybe(None) => bytes.push(2),
            SymbolicResultField::Maybe(Some(row)) => {
                bytes.push(3);
                hash_observation_row(&mut bytes, row)?;
            }
            SymbolicResultField::Many(rows) => {
                bytes.push(4);
                bytes.extend_from_slice(&u32::try_from(rows.len()).ok()?.to_be_bytes());
                for row in rows {
                    hash_observation_row(&mut bytes, row)?;
                }
            }
        }
    }
    Some(hash_generated_artifact(&bytes))
}

fn hash_observation_row(bytes: &mut Vec<u8>, row: &SymbolicResultRecord) -> Option<()> {
    push_observation_bytes(bytes, row.entity().as_bytes())?;
    let count = row
        .fields()
        .len()
        .checked_add(row.nested().len())?
        .checked_add(row.exact_decimals().len())?
        .checked_add(row.exact_means().len())?;
    bytes.extend_from_slice(&u32::try_from(count).ok()?.to_be_bytes());
    for (name, value) in row.fields() {
        push_observation_bytes(bytes, name.as_bytes())?;
        bytes.push(1);
        let value = encode_canonical_value(value).ok()?;
        push_observation_bytes(bytes, &value)?;
    }
    for (name, value) in row.exact_decimals() {
        push_observation_bytes(bytes, name.as_bytes())?;
        bytes.push(2);
        bytes.extend_from_slice(&value.coefficient().to_be_bytes());
        bytes.push(value.scale());
    }
    for (name, value) in row.exact_means() {
        push_observation_bytes(bytes, name.as_bytes())?;
        bytes.push(3);
        bytes.extend_from_slice(&value.coefficient().to_be_bytes());
        bytes.push(value.scale());
        bytes.extend_from_slice(&value.count().to_be_bytes());
    }
    for (name, rows) in row.nested() {
        push_observation_bytes(bytes, name.as_bytes())?;
        bytes.push(4);
        bytes.extend_from_slice(&u32::try_from(rows.len()).ok()?.to_be_bytes());
        for nested in rows {
            hash_observation_row(bytes, nested)?;
        }
    }
    Some(())
}

fn push_observation_bytes(target: &mut Vec<u8>, value: &[u8]) -> Option<()> {
    target.extend_from_slice(&u32::try_from(value.len()).ok()?.to_be_bytes());
    target.extend_from_slice(value);
    Some(())
}

pub(crate) async fn load_query_module(
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
        Arc::from([]),
        Arc::new(compiled.document),
        None,
        None,
        None,
        QueryAuthority::AdHoc,
        request.parameters,
        request.cursor,
        request.minimum_application_head,
        request.consistency,
        false,
        false,
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

fn resume_query_consistency(
    prior: &QueryCursorState,
    requested_floor: Option<u64>,
    requested_consistency: Option<QueryConsistencyV1>,
) -> Option<(Option<u64>, bool)> {
    if (requested_consistency == Some(QueryConsistencyV1::AdmissionHead)
        && !prior.admission_head_fenced())
        || requested_floor.is_some_and(|minimum| {
            prior
                .minimum_application_head()
                .is_none_or(|frozen| minimum > frozen)
        })
    {
        return None;
    }
    Some((
        prior.minimum_application_head(),
        prior.admission_head_fenced(),
    ))
}

fn effective_query_consistency(
    has_candidates: bool,
    requested: Option<QueryConsistencyV1>,
) -> Option<QueryConsistencyV1> {
    if has_candidates {
        Some(QueryConsistencyV1::AdmissionHead)
    } else {
        requested
    }
}

fn continuation_provider_epoch(
    prior: Option<&QueryContinuation>,
) -> Result<Option<CommitSequence>, QueryExecutionError> {
    let mut epoch = None;
    if let Some(prior) = prior {
        for (identity, value) in prior.index_epochs() {
            if !identity.starts_with("provider.") {
                continue;
            }
            let observed = CommitSequence::new(*value).ok_or(QueryExecutionError::StaleCursor)?;
            match epoch {
                None => epoch = Some(observed),
                Some(existing) if existing == observed => {}
                Some(_) => return Err(QueryExecutionError::StaleCursor),
            }
        }
    }
    Ok(epoch)
}

enum LongPatternPreparationError {
    Query(QueryExecutionError),
    Provider(ExactTextProjectionPortError),
}

impl From<QueryExecutionError> for LongPatternPreparationError {
    fn from(error: QueryExecutionError) -> Self {
        Self::Query(error)
    }
}

const fn long_pattern_readiness_error(
    error: &LongPatternPreparationError,
) -> Option<ExactTextProjectionPortError> {
    match error {
        LongPatternPreparationError::Provider(error) => Some(*error),
        LongPatternPreparationError::Query(_) => None,
    }
}

#[cfg(test)]
mod provider_readiness_tests {
    use super::*;

    struct ImmediateReadinessScheduler {
        request_deadline: Instant,
    }

    impl crate::RequestDeadlineScheduler for ImmediateReadinessScheduler {
        fn wait_until(&self, deadline: Instant) -> crate::RequestDeadlineFuture<'_> {
            if deadline == self.request_deadline {
                Box::pin(std::future::pending())
            } else {
                Box::pin(async {})
            }
        }
    }

    fn block_on<F: std::future::Future>(future: F) -> F::Output {
        tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .expect("runtime")
            .block_on(future)
    }

    #[test]
    fn first_long_pattern_candidate_waits_for_worker_publication() {
        let deadline = Instant::now()
            .checked_add(Duration::from_secs(1))
            .expect("deadline");
        let (control, _cancellation) = crate::RequestControl::new(deadline);
        let scheduler = ImmediateReadinessScheduler {
            request_deadline: deadline,
        };
        let mut attempts = 0_u8;
        let mut reauthorizations = 0_u8;

        let result = block_on(execute_provider_with_readiness(
            &control,
            &scheduler,
            || {
                attempts = attempts.saturating_add(1);
                if attempts < 3 {
                    Err(LongPatternPreparationError::Provider(
                        ExactTextProjectionPortError::Building,
                    ))
                } else {
                    Ok(17_u8)
                }
            },
            long_pattern_readiness_error,
            || {
                reauthorizations = reauthorizations.saturating_add(1);
                async { Ok(()) }
            },
        ))
        .expect("bounded readiness wait");
        let result = match result {
            Ok(result) => result,
            Err(_) => panic!("provider did not publish within the readiness wait"),
        };

        assert_eq!(result, 17);
        assert_eq!(attempts, 3);
        assert_eq!(reauthorizations, 2);
    }

    #[test]
    fn query_preparation_failure_is_not_a_provider_readiness_retry() {
        let deadline = Instant::now()
            .checked_add(Duration::from_secs(1))
            .expect("deadline");
        let (control, _cancellation) = crate::RequestControl::new(deadline);
        let scheduler = ImmediateReadinessScheduler {
            request_deadline: deadline,
        };
        let mut attempts = 0_u8;
        let mut reauthorizations = 0_u8;

        let result = block_on(execute_provider_with_readiness(
            &control,
            &scheduler,
            || {
                attempts = attempts.saturating_add(1);
                Err::<u8, _>(LongPatternPreparationError::Query(
                    QueryExecutionError::InvalidProgram,
                ))
            },
            long_pattern_readiness_error,
            || {
                reauthorizations = reauthorizations.saturating_add(1);
                async { Ok(()) }
            },
        ))
        .expect("closed preparation result");

        assert!(matches!(
            result,
            Err(LongPatternPreparationError::Query(
                QueryExecutionError::InvalidProgram
            ))
        ));
        assert_eq!(attempts, 1);
        assert_eq!(reauthorizations, 0);
    }
}

fn prepare_long_pattern_participants(
    authorization: &AuthorizedApplicationQuery,
    provider: &dyn crate::LongPatternProjectionPort,
    program: &QueryAccessProgramV1,
    parameters: &QueryParameters,
    prior: Option<&QueryContinuation>,
    row_policy: Option<&Arc<AuthorizedQueryRowPolicyContextV1>>,
    minimum_application_head: Option<u64>,
) -> Result<
    (
        riffdb_projection::ResultSetEpochProofV1,
        Vec<LongPatternCandidateBatch>,
        riffdb_types::ApplicationRoleHash,
    ),
    LongPatternPreparationError,
> {
    let policy_shape = authorization
        .application_role_hash()
        .ok_or(QueryExecutionError::InvalidProgram)?;
    let partition_value = parameters
        .get(program.partition_parameter())
        .ok_or_else(|| QueryExecutionError::MissingParameter {
            parameter: program.partition_parameter().to_owned(),
        })?;
    let route_values = match program.partition_route() {
        riffdb_query_ir::QueryPartitionRouteV1::Exact { .. } => {
            std::slice::from_ref(partition_value)
        }
        riffdb_query_ir::QueryPartitionRouteV1::FiniteSet { maximum, .. } => {
            let CanonicalValue::List(values) = partition_value else {
                return Err(QueryExecutionError::InvalidParameter {
                    parameter: program.partition_parameter().to_owned(),
                }
                .into());
            };
            if values.values().is_empty() || values.values().len() > usize::from(*maximum) {
                return Err(QueryExecutionError::InvalidParameter {
                    parameter: program.partition_parameter().to_owned(),
                }
                .into());
            }
            values.values()
        }
    };
    let target_partitions = authorization.target().partitions();
    if target_partitions.len() != route_values.len() {
        return Err(QueryExecutionError::InvalidProgram.into());
    }
    let minimum_epoch = match minimum_application_head {
        Some(value) => Some(CommitSequence::new(value).ok_or(QueryExecutionError::InvalidProgram)?),
        None => None,
    };
    let pinned_epoch = continuation_provider_epoch(prior)?;
    let mut batches = Vec::new();
    let mut participants = Vec::<ProviderEpochObservationV1>::new();
    let mut pending_provider = None;
    for step in program.steps() {
        let QueryAccessKind::LongPatternCandidate { pattern, .. } = step.access() else {
            continue;
        };
        let CanonicalValue::String(source_pattern) = parameters
            .get(pattern.pattern_parameter())
            .ok_or_else(|| QueryExecutionError::MissingParameter {
                parameter: pattern.pattern_parameter().to_owned(),
            })?
        else {
            return Err(QueryExecutionError::InvalidParameter {
                parameter: pattern.pattern_parameter().to_owned(),
            }
            .into());
        };
        let compiled = CompiledLongPatternV1::compile_bounded(
            pattern.operator(),
            pattern.profile(),
            source_pattern.as_str(),
            pattern.bounds(),
        )
        .map_err(|_| QueryExecutionError::InvalidParameter {
            parameter: pattern.pattern_parameter().to_owned(),
        })?;
        let mut partition_batches = Vec::with_capacity(route_values.len());
        let mut step_pending = false;
        for (partition, partition_value) in target_partitions.iter().zip(route_values) {
            let request = LongPatternProjectionRequest::new(
                Arc::new(program.clone()),
                step.clone(),
                partition.partition_key().clone(),
                partition_value.clone(),
                policy_shape,
                row_policy.cloned(),
                compiled.clone(),
                minimum_epoch,
                pinned_epoch,
            );
            let batch = match provider.execute(request) {
                Ok(batch) => batch,
                Err(error) if exact_provider_readiness_pending(error) => {
                    pending_provider.get_or_insert(error);
                    step_pending = true;
                    continue;
                }
                Err(error) => return Err(LongPatternPreparationError::Provider(error)),
            };
            if batch.binding() != step.binding() {
                return Err(QueryExecutionError::BackendIntegrity.into());
            }
            partition_batches.push(batch);
        }
        if step_pending {
            continue;
        }
        let route_count =
            u64::try_from(route_values.len()).map_err(|_| QueryExecutionError::BoundExceeded)?;
        let batch = LongPatternCandidateBatch::merge_partition_batches(
            partition_batches,
            step.maximum_rows(),
            u64::from(pattern.bounds().rows())
                .checked_mul(route_count)
                .ok_or(QueryExecutionError::BoundExceeded)?,
            pattern
                .bounds()
                .verification_bytes()
                .checked_mul(route_count)
                .ok_or(QueryExecutionError::BoundExceeded)?,
        )
        .ok_or(QueryExecutionError::BoundExceeded)?;
        let observation = batch
            .observation()
            .map_err(|_| QueryExecutionError::BackendIntegrity)?;
        if let Some(existing) = participants
            .iter()
            .find(|existing| existing.descriptor() == observation.descriptor())
        {
            if existing != &observation {
                return Err(QueryExecutionError::BackendIntegrity.into());
            }
        } else {
            participants.push(observation);
        }
        batches.push(batch);
    }
    if let Some(error) = pending_provider {
        return Err(LongPatternPreparationError::Provider(error));
    }
    let requirement = pinned_epoch.map_or_else(
        || {
            minimum_epoch.map_or(
                ResultSetEpochRequirementV1::Latest,
                ResultSetEpochRequirementV1::AtLeast,
            )
        },
        ResultSetEpochRequirementV1::Exact,
    );
    let proof = negotiate_result_set_epoch_v1(
        ResultSetEpochContextV1::new(program.identity().hash(), policy_shape),
        &participants,
        requirement,
    )
    .map_err(|error| match error {
        riffdb_projection::ResultSetEpochError::EpochExpired => QueryExecutionError::StaleCursor,
        riffdb_projection::ResultSetEpochError::InvalidInterval
        | riffdb_projection::ResultSetEpochError::IncarnationMismatch
        | riffdb_projection::ResultSetEpochError::EmptyParticipants
        | riffdb_projection::ResultSetEpochError::TooManyParticipants => {
            QueryExecutionError::BackendIntegrity
        }
        riffdb_projection::ResultSetEpochError::Diverged
        | riffdb_projection::ResultSetEpochError::FreshnessUnavailable
        | riffdb_projection::ResultSetEpochError::Rebuilding
        | riffdb_projection::ResultSetEpochError::Retired
        | riffdb_projection::ResultSetEpochError::Unavailable => {
            QueryExecutionError::BackendUnavailable
        }
    })?;
    Ok((proof, batches, policy_shape))
}

#[allow(clippy::too_many_arguments)]
async fn execute_compiled_query(
    service: Arc<RiffDbServiceInner>,
    context: RequestContext,
    bundle: riffdb_catalog::ValidatedContractBundle,
    program: Arc<QueryAccessProgramV1>,
    aggregates: Arc<[riffdb_query_ir::OperationalAggregateV1]>,
    document: Arc<Document>,
    module_hash: Option<QueryModuleHash>,
    named_response_plan_hash: Option<QueryPlanHash>,
    named_cursor_plan_hash: Option<QueryPlanHash>,
    authority: QueryAuthority,
    submitted: SymbolicQueryParameters,
    cursor: Option<CursorToken>,
    minimum_application_head: Option<u64>,
    consistency: Option<QueryConsistencyV1>,
    retain_compact_result: bool,
    retain_packed_result: bool,
) -> ServiceResult<ExecuteSymbolicQueryResult> {
    const OPERATION: ServiceOperationV1 = ServiceOperationV1::ExecuteQuery;
    if program.steps().len() > MAX_SYMBOLIC_QUERY_STEPS {
        return Err(validation_failure(ValidationCode::InvalidValue));
    }
    // Candidate sources must observe one admission-fenced participant set.
    // This is compiler-derived consistency, never an optional caller
    // downgrade. Ordinary queries preserve the submitted consistency mode.
    let consistency =
        effective_query_consistency(!program.surface().candidates().is_empty(), consistency);
    let param_materialize_started = Instant::now();
    let parameters =
        materialize_query_parameters(&service, OPERATION, bundle.bundle(), &document, &submitted)?;
    let cursor_parameter_hash = query_cursor_parameter_hash(&program, &parameters)
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
        named_cursor_plan_hash.unwrap_or_else(|| program.identity().hash()),
        cursor_parameter_hash,
        context.principal().capability_id(),
        context.principal().capability_revision(),
    );
    service
        .providers
        .telemetry
        .record(ServiceTelemetryEvent::ReadPipelineStageCompleted {
            stage: ReadPipelineStage::ParamMaterialize,
            elapsed: param_materialize_started.elapsed(),
        });
    let authorize_begin_started = Instant::now();
    let begun = begin_symbolic(&service, &context, &bundle, operation_request, OPERATION).await?;
    service
        .providers
        .telemetry
        .record(ServiceTelemetryEvent::ReadPipelineStageCompleted {
            stage: ReadPipelineStage::AuthorizeBegin,
            elapsed: authorize_begin_started.elapsed(),
        });
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
    let empty_partition_set = program.partition_route().is_finite_set()
        && matches!(
            parameters.get(program.partition_parameter()),
            Some(CanonicalValue::List(values)) if values.values().is_empty()
        );
    let has_long_pattern = !empty_partition_set
        && program
            .steps()
            .iter()
            .any(|step| matches!(step.access(), QueryAccessKind::LongPatternCandidate { .. }));
    let long_pattern_provider = if has_long_pattern {
        let Some(provider) = service.providers.long_pattern.as_ref() else {
            let failure = PublicError::storage_unavailable().into();
            return Err(finish_failure(&service, &context, &begun, failure).await);
        };
        Some(Arc::clone(provider))
    } else {
        None
    };
    let authorize_pre_started = Instant::now();
    // Read safe point 2. Revision-checked: reissues the begin proof only when
    // the capability view, the validity window, and the request are unchanged.
    let execution_authorization = begun
        .reauthorize_read(&service, &context)
        .await?
        .into_application_query()
        .ok_or_else(|| service.internal_failure(OPERATION, InternalDefect::ProofMismatch))?;
    let (minimum_application_head, admission_head_fenced) = match prior.as_deref() {
        Some(prior) => {
            let Some(frozen) =
                resume_query_consistency(prior, minimum_application_head, consistency)
            else {
                let failure = application_validation_failure(
                    ValidationCode::InvalidValue,
                    ApplicationErrorCode::CursorInvalid,
                );
                return Err(finish_failure(&service, &context, &begun, failure).await);
            };
            frozen
        }
        None => {
            let floor = match resolve_admission_head_floor(
                &service,
                &context,
                minimum_application_head,
                consistency,
            )
            .await
            {
                Ok(floor) => floor,
                Err(failure) => {
                    return Err(finish_failure(&service, &context, &begun, failure).await);
                }
            };
            (
                floor,
                consistency == Some(QueryConsistencyV1::AdmissionHead),
            )
        }
    };
    let row_policy =
        resolve_authorized_query_row_policy_context(&execution_authorization, bundle.bundle())
            .map_err(|_| service.internal_failure(OPERATION, InternalDefect::ProofMismatch))?
            .map(Arc::new);
    service
        .providers
        .telemetry
        .record(ServiceTelemetryEvent::ReadPipelineStageCompleted {
            stage: ReadPipelineStage::AuthorizePre,
            elapsed: authorize_pre_started.elapsed(),
        });

    // Query execution and continuation registration retry together; audit
    // begin/finish stay outside the loop.
    let (snapshot, cursor_guard) = match crate::read_retry::with_read_retry(
        context.control(),
        service.providers.deadline_scheduler.as_ref(),
        service.providers.telemetry.as_ref(),
        OPERATION,
        |_attempt| {
            let execution_authorization = &execution_authorization;
            let row_policy = &row_policy;
            let program = &program;
            let aggregates = &aggregates;
            let parameters = &parameters;
            let prior_cont = prior.as_deref().map(QueryCursorState::continuation);
            let cursor_lookup = cursor_lookup.clone();
            let principal = context.principal().principal_id().clone();
            let executor = executor.as_ref();
            let cursors = &service.cursors;
            let service = &service;
            let context = &context;
            let begun = &begun;
            let telemetry = service.providers.telemetry.as_ref();
            let captured_minimum_application_head = minimum_application_head;
            let long_pattern_provider = long_pattern_provider.as_deref();
            async move {
                let execute_started = Instant::now();
                let executed = if let Some(provider) = long_pattern_provider {
                    let prepared = execute_provider_with_readiness(
                        context.control(),
                        service.providers.deadline_scheduler.as_ref(),
                        || {
                            prepare_long_pattern_participants(
                                execution_authorization,
                                provider,
                                program,
                                parameters,
                                prior_cont,
                                row_policy.as_ref(),
                                captured_minimum_application_head,
                            )
                        },
                        long_pattern_readiness_error,
                        || async {
                            begun.reauthorize_read(service, context).await?;
                            Ok(())
                        },
                    )
                    .await;
                    let prepared = match prepared {
                        Ok(prepared) => prepared,
                        Err(failure) => return Err(Err(failure)),
                    };
                    match prepared {
                        Ok((proof, batches, policy_shape)) => {
                            execute_authorized_provider_query_page(
                                execution_authorization,
                                executor,
                                program,
                                aggregates,
                                parameters,
                                prior_cont,
                                row_policy.as_deref(),
                                policy_shape,
                                &proof,
                                &batches,
                            )
                        }
                        Err(LongPatternPreparationError::Query(error)) => Err(error),
                        Err(LongPatternPreparationError::Provider(error)) => {
                            let Some(code) = exact_projection_application_code(error) else {
                                return Err(Err(service
                                    .internal_failure(OPERATION, InternalDefect::ProofMismatch)));
                            };
                            return Err(Err(application_validation_failure(
                                ValidationCode::InvalidValue,
                                code,
                            )));
                        }
                    }
                } else {
                    execute_authorized_query_page(
                        execution_authorization,
                        executor,
                        program,
                        aggregates,
                        parameters,
                        prior_cont,
                        row_policy.as_deref(),
                    )
                };
                let snapshot = match executed {
                    Ok(snapshot) => {
                        telemetry.record(ServiceTelemetryEvent::ReadPipelineStageCompleted {
                            stage: ReadPipelineStage::Execute,
                            elapsed: execute_started.elapsed(),
                        });
                        snapshot
                    }
                    Err(QueryExecutionError::BackendUnavailable) => {
                        return Err(Ok(
                            crate::read_retry::RetryableReadFault::BackendUnavailable,
                        ));
                    }
                    Err(error) => {
                        return Err(Err(execution_failure(service, OPERATION, error)));
                    }
                };
                if captured_minimum_application_head
                    .is_some_and(|minimum| snapshot.application_head() < minimum)
                {
                    return Err(Err(application_validation_failure(
                        ValidationCode::InvalidValue,
                        ApplicationErrorCode::FreshnessUnsatisfied,
                    )));
                }
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
                            QueryCursorState::new(
                                continuation,
                                captured_minimum_application_head,
                                admission_head_fenced,
                            ),
                        ) {
                            Ok(guard) => {
                                if guard.capacity_evicted() {
                                    telemetry.record(ServiceTelemetryEvent::CursorEvicted);
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
    let authorize_post_started = Instant::now();
    // Read safe point 3, same revision-checked contract as safe point 2.
    begun.reauthorize_read(&service, &context).await?;
    service
        .providers
        .telemetry
        .record(ServiceTelemetryEvent::ReadPipelineStageCompleted {
            stage: ReadPipelineStage::AuthorizePost,
            elapsed: authorize_post_started.elapsed(),
        });
    let response_build_started = Instant::now();
    let mut result = ExecuteSymbolicQueryResult::from_snapshot(
        &program,
        snapshot,
        Arc::clone(bundle.enum_variant_names()),
        retain_compact_result && module_hash.is_some(),
        retain_packed_result && module_hash.is_some(),
    );
    if let Some(module_hash) = module_hash {
        result.identity = SymbolicQueryIdentity::from_named_plan(
            &program,
            module_hash,
            named_response_plan_hash.unwrap_or_else(|| program.identity().hash()),
        );
    }
    service
        .providers
        .telemetry
        .record(ServiceTelemetryEvent::ReadPipelineStageCompleted {
            stage: ReadPipelineStage::ResponseBuild,
            elapsed: response_build_started.elapsed(),
        });
    let audit_finish_started = Instant::now();
    finish_success(&service, &context, &begun).await?;
    result.next_cursor = cursor_guard.map(crate::CursorPublicationGuard::publish);
    service
        .providers
        .telemetry
        .record(ServiceTelemetryEvent::ReadPipelineStageCompleted {
            stage: ReadPipelineStage::AuditFinish,
            elapsed: audit_finish_started.elapsed(),
        });
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

#[cfg(test)]
mod admission_head_cursor_tests {
    use super::*;

    fn state(floor: Option<u64>, fenced: bool) -> QueryCursorState {
        QueryCursorState::new(
            QueryContinuation::checked(
                "items".to_owned(),
                vec![1],
                BTreeMap::from([("Item.by_id".to_owned(), 7)]),
            )
            .expect("bounded continuation"),
            floor,
            fenced,
        )
    }

    #[test]
    fn continuation_consistency_is_frozen_without_recapture_or_upgrade() {
        let fenced = state(Some(42), true);
        assert_eq!(
            resume_query_consistency(&fenced, None, None),
            Some((Some(42), true))
        );
        assert_eq!(
            resume_query_consistency(&fenced, Some(41), Some(QueryConsistencyV1::AdmissionHead)),
            Some((Some(42), true)),
            "a repeated stronger option is idempotent and retains the first floor"
        );
        assert_eq!(
            resume_query_consistency(&fenced, Some(43), None),
            None,
            "a continuation cannot raise its frozen floor"
        );

        let legacy = state(None, false);
        assert_eq!(
            resume_query_consistency(&legacy, None, Some(QueryConsistencyV1::AdmissionHead)),
            None,
            "a legacy cursor cannot be upgraded in place"
        );
    }

    #[test]
    fn candidate_queries_cannot_downgrade_the_admission_fence() {
        assert_eq!(
            effective_query_consistency(true, None),
            Some(QueryConsistencyV1::AdmissionHead)
        );
        assert_eq!(
            effective_query_consistency(true, Some(QueryConsistencyV1::AdmissionHead)),
            Some(QueryConsistencyV1::AdmissionHead)
        );
        assert_eq!(effective_query_consistency(false, None), None);
    }
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
            (TypeReference::Optional(_), None | Some(SubmittedValue::Null)) => continue,
            (TypeReference::Optional(inner), Some(value)) => {
                let value_type = query_value_type(bundle, &inner.value).ok_or_else(|| {
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
            (TypeReference::Limit, None) if parameter.default.is_some() => continue,
            (TypeReference::BoundedLimit(_), None) if parameter.default.is_some() => continue,
            (_, None) => return Err(validation_failure(ValidationCode::InvalidValue)),
            (TypeReference::Limit, Some(SubmittedValue::U64(value)))
                if *value >= 1 && riffdb_query_executor::page_take_within_scan_bound(*value) =>
            {
                canonical.insert(name.to_owned(), CanonicalValue::U64(*value));
            }
            (TypeReference::Limit, Some(_)) => {
                return Err(validation_failure(ValidationCode::TypeMismatch));
            }
            (TypeReference::BoundedLimit(maximum), Some(SubmittedValue::U64(value)))
                if *value >= 1
                    && *value <= *maximum
                    && riffdb_query_executor::page_take_within_scan_bound(*value) =>
            {
                canonical.insert(name.to_owned(), CanonicalValue::U64(*value));
            }
            (TypeReference::BoundedLimit(_), Some(_)) => {
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
            (
                TypeReference::BoundedSet { element, maximum },
                Some(SubmittedValue::List(values)),
            ) if values.values().len() <= usize::from(*maximum) => {
                let value_type = query_value_type(bundle, &element.value).ok_or_else(|| {
                    service.internal_failure(operation, InternalDefect::ProofMismatch)
                })?;
                let mut values = values
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
                    .collect::<ServiceResult<Vec<_>>>()?
                    .into_iter()
                    .map(|value| {
                        let encoded = encode_canonical_value(&value).map_err(|_| {
                            service.internal_failure(operation, InternalDefect::ProofMismatch)
                        })?;
                        Ok((encoded, value))
                    })
                    .collect::<ServiceResult<Vec<_>>>()?;
                values.sort_unstable_by(|left, right| left.0.cmp(&right.0));
                values.dedup_by(|left, right| left.0 == right.0);
                if values.len() > usize::from(*maximum) {
                    return Err(validation_failure(ValidationCode::InvalidValue));
                }
                let value =
                    CanonicalValue::list(values.into_iter().map(|(_, value)| value).collect())
                        .map_err(|_| validation_failure(ValidationCode::InvalidValue))?;
                canonical.insert(name.to_owned(), value);
            }
            (TypeReference::BoundedSet { .. }, Some(_)) => {
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
            [enumeration] if enumeration.value.as_str() == "u64" => {
                Some(riffdb_contract_ir::ValueType::u64())
            }
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
        TypeReference::BoundedString(maximum) => {
            riffdb_contract_ir::ValueType::string((*maximum).try_into().ok()?).ok()
        }
        TypeReference::Set(_)
        | TypeReference::BoundedSet { .. }
        | TypeReference::Cursor
        | TypeReference::Limit
        | TypeReference::BoundedLimit(_) => None,
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

pub(crate) fn query_parameter_hash(
    parameters: &QueryParameters,
) -> Option<riffdb_types::QueryParameterHash> {
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

fn query_cursor_parameter_hash(
    program: &QueryAccessProgramV1,
    parameters: &QueryParameters,
) -> Option<riffdb_types::QueryParameterHash> {
    let cardinality_parameters = program.cursor_page_cardinality_parameters();
    let included_count = parameters
        .iter()
        .filter(|(name, _)| cardinality_parameters.binary_search(name).is_err())
        .count();
    let mut bytes = Vec::new();
    bytes.extend_from_slice(b"RDBQCURSORPARAM\x01");
    bytes.extend_from_slice(&u32::try_from(included_count).ok()?.to_be_bytes());
    for (name, value) in parameters.iter() {
        if cardinality_parameters.binary_search(&name).is_ok() {
            continue;
        }
        let name = name.as_bytes();
        bytes.extend_from_slice(&u32::try_from(name.len()).ok()?.to_be_bytes());
        bytes.extend_from_slice(name);
        let value = encode_canonical_value(value).ok()?;
        bytes.extend_from_slice(&u32::try_from(value.len()).ok()?.to_be_bytes());
        bytes.extend_from_slice(&value);
    }
    Some(hash_query_parameters(&bytes))
}

pub(crate) fn application_query_target(
    bundle: &riffdb_contract_ir::ContractBundle,
    program: &QueryAccessProgramV1,
    parameters: &QueryParameters,
    ingress: riffdb_types::ServiceIngressKindV1,
) -> Option<ApplicationQueryTarget> {
    application_query_target_with_identity(
        bundle,
        program,
        parameters,
        ingress,
        program.identity().hash(),
        program.cost(),
        None,
    )
}

fn application_query_target_with_identity(
    bundle: &riffdb_contract_ir::ContractBundle,
    program: &QueryAccessProgramV1,
    parameters: &QueryParameters,
    ingress: riffdb_types::ServiceIngressKindV1,
    plan_hash: QueryPlanHash,
    cost: riffdb_types::QueryCostVectorV1,
    maximum_access_rows_override: Option<NonZeroU16>,
) -> Option<ApplicationQueryTarget> {
    let partition_value = parameters.get(program.partition_parameter())?;
    let partition_values = match program.partition_route() {
        riffdb_query_ir::QueryPartitionRouteV1::Exact { .. } => {
            std::slice::from_ref(partition_value)
        }
        riffdb_query_ir::QueryPartitionRouteV1::FiniteSet { maximum, .. } => {
            let CanonicalValue::List(values) = partition_value else {
                return None;
            };
            if values.values().len() > usize::from(*maximum) {
                return None;
            }
            values.values()
        }
    };
    let mut routed_partitions = None;
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
        let partitions = {
            let aggregate = bundle
                .schema()
                .aggregate_for_entity(step.internal_entity_id())?;
            partition_values
                .iter()
                .map(|partition_value| {
                    aggregate
                        .keys()
                        .partition_schema()
                        .encode_partition(std::slice::from_ref(partition_value))
                        .ok()
                })
                .collect::<Option<Vec<_>>>()?
        };
        // PartitionKey retains the aggregate owner as a namespace. A
        // compiler-proved composite query may cross aggregate owners while
        // every step is still routed by this exact canonical parameter. Keep
        // the first key as the authorization route anchor; successful encoding
        // through every aggregate schema proves the shared typed route value.
        if routed_partitions.is_none() {
            routed_partitions = Some(partitions);
        }
        // Exact provider execution performs no authoritative entity/index
        // scan on the request path. Its access requirement names the entity,
        // index, and visible fields whose already-proved derived descriptor is
        // consumed, while the output/candidate ceilings remain in the sealed
        // cost vector. Represent that single descriptor access explicitly so
        // background provider work never inflates application scan authority.
        let rows = match maximum_access_rows_override {
            Some(rows) => rows,
            None => {
                let maximum = match step.access() {
                    QueryAccessKind::PartitionSetIndex { scan_ceiling, .. } => {
                        u64::from(*scan_ceiling)
                    }
                    _ => step.maximum_rows(),
                };
                let Ok(rows) = u16::try_from(maximum) else {
                    return None;
                };
                NonZeroU16::new(rows)?
            }
        };
        let index_id = match step.access() {
            QueryAccessKind::Index { .. }
            | QueryAccessKind::ExpansionIndex { .. }
            | QueryAccessKind::PartitionSetIndex { .. }
            | QueryAccessKind::LongPatternCandidate { .. } => Some(step.internal_index_id()?),
            QueryAccessKind::Point { .. }
            | QueryAccessKind::DependentPointBatch { .. }
            | QueryAccessKind::Nearest { .. }
            | QueryAccessKind::CandidateRootHydration { .. } => None,
        };
        // ADR-0118: secret-classified fields are gated on PROJECTION only.
        // The step's selected fields — the ones its released rows carry —
        // resolve against the schema's secret set and require the grant's
        // dedicated secret naming. Predicate-only secret use compares the
        // value without returning it (the executor's projection step drops
        // non-selected fields), so secrets leave the ordinary non-key list
        // and its all-fields-visible rule entirely.
        let schema_secret_fields = bundle
            .schema()
            .secret_fields_for_entity(step.internal_entity_id());
        let (projected_secret_fields, non_key_fields) = if schema_secret_fields.is_empty() {
            (Vec::new(), non_key_fields)
        } else {
            let field_ids: std::collections::BTreeMap<&str, riffdb_types::FieldId> =
                entity.internal_fields().collect();
            let mut projected: Vec<riffdb_types::FieldId> = step
                .selected_fields()
                .iter()
                .filter_map(|name| field_ids.get(name.as_str()).copied())
                .filter(|field| schema_secret_fields.binary_search(field).is_ok())
                .collect();
            projected.sort_unstable();
            projected.dedup();
            let ordinary = non_key_fields
                .into_iter()
                .filter(|field| schema_secret_fields.binary_search(field).is_err())
                .collect();
            (projected, ordinary)
        };
        let access = match program.partition_route() {
            riffdb_query_ir::QueryPartitionRouteV1::Exact { .. } => {
                ApplicationQueryAccessRequirement::new(
                    step.internal_entity_id(),
                    index_id,
                    non_key_fields,
                    rows,
                )
            }
            riffdb_query_ir::QueryPartitionRouteV1::FiniteSet { maximum, .. } => {
                ApplicationQueryAccessRequirement::new_partition_set(
                    step.internal_entity_id(),
                    index_id,
                    non_key_fields,
                    rows,
                    NonZeroU16::new(*maximum)?,
                )
            }
        };
        accesses.push(
            access
                .and_then(|access| access.with_projected_secret_fields(projected_secret_fields))
                .ok()?,
        );
    }
    let target = match program.partition_route() {
        riffdb_query_ir::QueryPartitionRouteV1::Exact { .. } => ApplicationQueryTarget::new(
            program.contract().lineage().clone(),
            program.contract().version(),
            program.contract().bundle_hash(),
            plan_hash,
            ingress,
            OperationTenantScope::global_only(),
            routed_partitions?.into_iter().next()?,
            accesses,
            cost,
        ),
        riffdb_query_ir::QueryPartitionRouteV1::FiniteSet { .. } => {
            ApplicationQueryTarget::new_partition_set(
                program.contract().lineage().clone(),
                program.contract().version(),
                program.contract().bundle_hash(),
                plan_hash,
                ingress,
                OperationTenantScope::global_only(),
                routed_partitions?,
                accesses,
                cost,
            )
        }
    }
    .ok()?;
    Some(if program.surface().candidates().is_empty() {
        target
    } else {
        target.with_bounded_candidate_sources()
    })
}

pub(crate) fn execute_authorized_query_page(
    authorization: &AuthorizedApplicationQuery,
    executor: &dyn riffdb_query_executor::QueryExecutionPort,
    program: &QueryAccessProgramV1,
    aggregates: &[riffdb_query_ir::OperationalAggregateV1],
    parameters: &QueryParameters,
    prior: Option<&QueryContinuation>,
    row_policy: Option<&AuthorizedQueryRowPolicyContextV1>,
) -> Result<QueryOwnedSnapshot, QueryExecutionError> {
    if program
        .steps()
        .iter()
        .any(|step| matches!(step.access(), QueryAccessKind::LongPatternCandidate { .. }))
    {
        return Err(QueryExecutionError::BackendUnavailable);
    }
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
        && obligations.partition_constraint() == Some(&query_partition_constraint(target));
    if !exact_target {
        return Err(QueryExecutionError::InvalidProgram);
    }
    match (aggregates.is_empty(), row_policy) {
        (true, Some(policy)) => {
            executor.execute_policy_query_page(program, parameters, prior, policy)
        }
        (false, Some(policy)) => executor
            .execute_policy_operational_query_page(program, aggregates, parameters, prior, policy),
        (true, None) => executor.execute_query_page(program, parameters, prior),
        (false, None) => {
            executor.execute_operational_query_page(program, aggregates, parameters, prior)
        }
    }
}

fn query_partition_constraint(target: &ApplicationQueryTarget) -> PartitionConstraint {
    match target.partitions() {
        [partition] => PartitionConstraint::Exact(partition.clone()),
        partitions => PartitionConstraint::Explicit(partitions.to_vec()),
    }
}

#[allow(clippy::too_many_arguments)]
fn execute_authorized_provider_query_page(
    authorization: &AuthorizedApplicationQuery,
    executor: &dyn riffdb_query_executor::QueryExecutionPort,
    program: &QueryAccessProgramV1,
    aggregates: &[riffdb_query_ir::OperationalAggregateV1],
    parameters: &QueryParameters,
    prior: Option<&QueryContinuation>,
    row_policy: Option<&AuthorizedQueryRowPolicyContextV1>,
    policy_shape: riffdb_types::ApplicationRoleHash,
    proof: &riffdb_projection::ResultSetEpochProofV1,
    batches: &[LongPatternCandidateBatch],
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
        && obligations.partition_constraint() == Some(&query_partition_constraint(target));
    if !exact_target || !aggregates.is_empty() {
        return Err(QueryExecutionError::InvalidProgram);
    }
    match row_policy {
        Some(policy) => executor.execute_policy_provider_query_page(
            program,
            parameters,
            prior,
            policy,
            policy_shape,
            proof,
            batches,
        ),
        None => executor.execute_provider_query_page(
            program,
            parameters,
            prior,
            policy_shape,
            proof,
            batches,
        ),
    }
}

pub(crate) fn execution_failure(
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
        | QueryExecutionError::AggregateOverflow
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
        NamedTypeSchema::BoundedSet { element, maximum } => {
            format!("Set<{}, {maximum}>", render_named_type(element))
        }
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
        NamedTypeSchema::BoundedLimit { maximum } => format!("Limit<{maximum}>"),
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
        ValueTypeTag::Vector => {
            format!(
                "vector<{}>",
                value.vector_dimension().expect("tag checked").get()
            )
        }
    }
}

#[cfg(test)]
mod operational_cursor_identity_tests {
    use super::*;
    use riffdb_contract_compiler::compile_contract_source;
    use riffdb_query_compiler::compile_query;
    use riffdb_query_ir::SymbolicCatalog;
    use riffdb_query_module::{
        NamedQuerySource, QueryModule, QueryModuleCandidate, QueryModuleName, QueryModuleVersion,
    };
    use riffdb_riffql_syntax::parse_query;

    const CONTRACT: &str = r#"
contract CursorIdentity version 1 {
  entity Item {
    key (organization_id: uuid, item_id: uuid)
    field label: string<32>
    index by_item (organization_id, item_id)
  }
  aggregate Items {
    root Item
    partition_by organization_id
    conflict_key (organization_id, item_id)
  }
}
"#;

    const CURSOR_QUERY: &str = r#"
query CursorItems(
  $organization_id: Item.organization_id,
  $limit: Limit<100> = 50,
  $after: Cursor?,
) {
  candidates matching: Item.item_id
    from Item.item_id using by_item
    where organization_id == $organization_id
    within 65535 else IntegrityFailure
  many items from Item
    where organization_id == $organization_id && item_id in matching
    order by item_id asc
    take $limit after $after
  return Found { items: items { item_id label } }
  outcomes Found | IntegrityFailure
}
"#;

    const UNPAGED_QUERY: &str = r#"
query UnpagedItems(
  $organization_id: Item.organization_id,
  $limit: Limit<100> = 50,
) {
  many items from Item
    where organization_id == $organization_id
    order by item_id asc
    take $limit
  return Found { items: items { item_id label } }
  outcomes Found
}
"#;

    const PARTITION_SET_CONTRACT: &str = r#"
contract CursorPartitionSet version 1 {
  entity Run {
    key (experiment_id: u64, run_id: string<32>)
    field start_time: timestamp
    index by_start (experiment_id, start_time, run_id)
  }
  aggregate Runs {
    root Run
    partition_by experiment_id
    conflict_key (experiment_id, run_id)
  }
}
"#;

    const PARTITION_SET_QUERY: &str = r#"
query SearchRuns(
  $experiment_ids: Set<Run.experiment_id, 1000>,
  $limit: Limit<5000> = 100,
  $after: Cursor?,
) {
  many runs from Run
    where experiment_id in $experiment_ids
    order by start_time desc, run_id asc
    take $limit after $after
  return Found { runs: runs { experiment_id run_id start_time } }
  outcomes Found
}
"#;

    const ORDER_FAMILY_CONTRACT: &str = r#"
contract OrderIdentity version 1 {
  enum ItemOrder { LabelAsc, IdDesc }
  entity Item {
    key (organization_id: uuid, item_id: uuid)
    field label: string<32>
    index by_item (organization_id, item_id)
    index by_label (organization_id, label, item_id)
  }
  aggregate Items {
    root Item
    partition_by organization_id
    conflict_key (organization_id, item_id)
  }
}
"#;

    const ORDER_FAMILY_QUERY: &str = r#"
query OrderedItems(
  $organization_id: Item.organization_id,
  $order: ItemOrder,
  $limit: Limit<100> = 50,
  $after: Cursor?,
) {
  candidates matching: Item.item_id
    from Item.item_id using by_item
    where organization_id == $organization_id
    within 65535 else IntegrityFailure
  many items from Item
    where organization_id == $organization_id && item_id in matching
    order by $order {
      LabelAsc: label asc, item_id asc;
      IdDesc: item_id desc;
    }
    take $limit after $after
  return Found { items: items { item_id label } }
  outcomes Found | IntegrityFailure
}
"#;

    fn program(source: &str) -> QueryAccessProgramV1 {
        let bundle = compile_contract_source(CONTRACT).expect("contract");
        let catalog = SymbolicCatalog::from_bundle(&bundle).expect("catalog");
        let document = parse_query(source).expect("query source");
        compile_query(&document, &catalog).expect("query plan")
    }

    fn parameters(organization: u8, limit: u64) -> QueryParameters {
        QueryParameters::checked(BTreeMap::from([
            (
                "organization_id".to_owned(),
                CanonicalValue::Uuid([organization; 16]),
            ),
            ("limit".to_owned(), CanonicalValue::U64(limit)),
        ]))
        .expect("bounded parameters")
    }

    #[test]
    fn canonical_parameter_hash_changes_when_optional_filter_becomes_present() {
        let absent = QueryParameters::checked(BTreeMap::from([(
            "organization_id".to_owned(),
            CanonicalValue::Uuid([0x11; 16]),
        )]))
        .expect("bounded absent parameter set");
        let present = QueryParameters::checked(BTreeMap::from([
            (
                "organization_id".to_owned(),
                CanonicalValue::Uuid([0x11; 16]),
            ),
            ("status".to_owned(), CanonicalValue::I64(1)),
        ]))
        .expect("bounded present parameter set");

        assert_ne!(
            query_parameter_hash(&absent).expect("absent hash"),
            query_parameter_hash(&present).expect("present hash"),
            "a cursor from the absent family member must not resume the present member"
        );
    }

    #[test]
    fn cursor_hash_excludes_only_eligible_runtime_page_cardinality() {
        let cursor_program = program(CURSOR_QUERY);
        let one = parameters(0x11, 1);
        let maximum = parameters(0x11, 100);

        assert_ne!(
            query_parameter_hash(&one).expect("complete hash"),
            query_parameter_hash(&maximum).expect("complete hash"),
            "shared parameter identity remains value-complete"
        );
        assert_eq!(
            query_cursor_parameter_hash(&cursor_program, &one).expect("one-row cursor hash"),
            query_cursor_parameter_hash(&cursor_program, &maximum)
                .expect("maximum-row cursor hash"),
            "page cardinality does not identify the continuation position"
        );
        assert_ne!(
            query_cursor_parameter_hash(&cursor_program, &one).expect("original partition"),
            query_cursor_parameter_hash(&cursor_program, &parameters(0x22, 1))
                .expect("changed partition"),
            "partition and other semantic parameters remain bound"
        );

        let unpaged_program = program(UNPAGED_QUERY);
        assert_ne!(
            query_cursor_parameter_hash(&unpaged_program, &one).expect("unpaged one"),
            query_cursor_parameter_hash(&unpaged_program, &maximum).expect("unpaged maximum"),
            "an unpaged runtime limit remains identity-bearing"
        );

        let wider_program = program(&CURSOR_QUERY.replace("Limit<100>", "Limit<101>"));
        assert_ne!(
            cursor_program.identity(),
            wider_program.identity(),
            "the declared maximum remains bound by immutable plan identity"
        );
    }

    #[test]
    fn partition_set_cursor_hash_normalizes_routes_and_excludes_page_cardinality() {
        let bundle = compile_contract_source(PARTITION_SET_CONTRACT).expect("contract");
        let catalog = SymbolicCatalog::from_bundle(&bundle).expect("catalog");
        let program = compile_query(
            &parse_query(PARTITION_SET_QUERY).expect("query source"),
            &catalog,
        )
        .expect("query plan");
        let parameters = |routes, limit| {
            QueryParameters::checked(BTreeMap::from([
                (
                    "experiment_ids".to_owned(),
                    CanonicalValue::list(routes).expect("routes"),
                ),
                ("limit".to_owned(), CanonicalValue::U64(limit)),
            ]))
            .expect("parameters")
        };
        let first = parameters(vec![CanonicalValue::U64(2), CanonicalValue::U64(1)], 1);
        let resumed = parameters(
            vec![
                CanonicalValue::U64(1),
                CanonicalValue::U64(2),
                CanonicalValue::U64(1),
            ],
            100,
        );
        assert_eq!(program.cursor_page_cardinality_parameters(), ["limit"]);
        assert_eq!(
            query_cursor_parameter_hash(&program, &first),
            query_cursor_parameter_hash(&program, &resumed)
        );
    }

    #[test]
    fn order_family_response_pins_family_while_cursor_pins_selected_member() {
        let bundle = compile_contract_source(ORDER_FAMILY_CONTRACT).expect("contract");
        let module = QueryModule::compile(
            QueryModuleCandidate::new(
                QueryModuleName::new("ordered_items").expect("module name"),
                QueryModuleVersion::new(1).expect("module version"),
                vec![
                    NamedQuerySource::new("OrderedItems", ORDER_FAMILY_QUERY)
                        .expect("query source"),
                ],
            )
            .expect("module candidate"),
            &bundle,
        )
        .expect("module");
        let query = module.query("OrderedItems").expect("query");
        let family = query.order_family().expect("order family");
        let label = family.select_name("LabelAsc").expect("label member");
        let id = family.select_name("IdDesc").expect("id member");

        assert_eq!(
            label.program().cursor_page_cardinality_parameters(),
            ["limit"],
            "candidate-root pagination must permit a different valid continuation page size"
        );
        assert_eq!(id.program().cursor_page_cardinality_parameters(), ["limit"]);

        let (label_response, label_cursor) = named_query_plan_hashes(query, label.program());
        let (id_response, id_cursor) = named_query_plan_hashes(query, id.program());

        assert_eq!(label_response, query.plan().identity());
        assert_eq!(id_response, query.plan().identity());
        assert_ne!(label_cursor, id_cursor);
        assert_eq!(label_cursor, label.program().identity().hash());
        assert_eq!(id_cursor, id.program().identity().hash());
    }
}

#[cfg(test)]
mod exact_result_response_tests {
    use super::*;
    use crate::ExactTextProjectionRow;
    use riffdb_query_module::{
        NamedQuerySource, QueryModule, QueryModuleCandidate, QueryModuleName, QueryModuleVersion,
    };
    use riffdb_types::{CanonicalRecord, ProjectionGeneration};

    const CONTRACT: &str = r#"
contract ExactUsers version 1 {
  entity User {
    key (organization_id: uuid, user_id: uuid)
    field name: string<128>
    index by_name (organization_id, name, user_id) text_key(name, binary_utf8_v1)
  }
  aggregate Users {
    root User
    partition_by organization_id
    conflict_key (organization_id, user_id)
  }
}
"#;

    const QUERY: &str = r#"
query SearchUsers(
  $organization_id: User.organization_id,
  $needle: User.name,
  $limit: Limit<499> = 50,
  $offset: u64 = 0
) {
  many users from User
    where organization_id == $organization_id && name contains $needle
    order by name asc, user_id asc
    take $limit offset $offset
  aggregate total from users { exact_count() as value }
  return Found { users: users { user_id name } total: total { value } }
  outcomes Found
}
"#;

    const RICH_CONTRACT: &str = r#"
contract Directory version 1 {
  entity User {
    key (organization_id: uuid, user_id: uuid)
    field email: string<320>
    field state: string<32>
    field created_at: u64
    field reviewed_at: optional<timestamp>
    index by_email (organization_id, email, user_id) text_key(email, binary_utf8_v1)
    index by_state (organization_id, state, user_id)
    index by_created (organization_id, created_at, user_id)
    index by_reviewed (organization_id, reviewed_at, user_id) presence(reviewed_at)
  }
  aggregate Users {
    root User
    partition_by organization_id
    conflict_key (organization_id, user_id)
  }
}
"#;

    const RICH_QUERY: &str = r#"
query SearchUsers(
  $organization_id: User.organization_id,
  $states: Set<User.state>,
  $before: User.created_at,
  $limit: Limit<499> = 50,
  $offset: u64 = 0
) {
  many users from User
    where organization_id == $organization_id
      && state in $states
      && created_at < $before
      && exists reviewed_at
    order by created_at desc, user_id asc
    take $limit offset $offset
  aggregate total from users { exact_count() as value }
  return Found { users: users { user_id email state created_at reviewed_at } total: total { value } }
  outcomes Found
}
"#;

    #[test]
    fn exact_page_and_count_shape_share_one_epoch_without_client_shaping() {
        let bundle = riffdb_contract_compiler::compile_contract_source(CONTRACT).expect("contract");
        let module = QueryModule::compile(
            QueryModuleCandidate::new(
                QueryModuleName::new("exact_users").expect("module name"),
                QueryModuleVersion::new(1).expect("module version"),
                vec![NamedQuerySource::new("SearchUsers", QUERY).expect("query")],
            )
            .expect("candidate"),
            &bundle,
        )
        .expect("module");
        let query = module.query("SearchUsers").expect("compiled query");
        let exact = query.exact_text_result().expect("exact plan");
        let program = exact.representative_program();
        let authorization_parameters = QueryParameters::checked(BTreeMap::from([
            (
                "organization_id".to_owned(),
                CanonicalValue::Uuid([0x11; 16]),
            ),
            (
                "needle".to_owned(),
                CanonicalValue::string("Ada").expect("needle"),
            ),
            ("limit".to_owned(), CanonicalValue::U64(50)),
            ("offset".to_owned(), CanonicalValue::U64(0)),
        ]))
        .expect("authorization parameters");
        let authorization_target = application_query_target_with_identity(
            &bundle,
            program,
            &authorization_parameters,
            riffdb_types::ServiceIngressKindV1::Grpc,
            exact.identity(),
            exact.authorization_cost(),
            Some(NonZeroU16::MIN),
        )
        .expect("exact authorization target");
        assert_eq!(authorization_target.cost().scanned_index_rows(), 0);
        assert_eq!(
            authorization_target.accesses()[0].maximum_rows(),
            NonZeroU16::MIN,
            "provider maintenance must not become request-time scan authority"
        );
        let step = &program.steps()[0];
        let access = program
            .internal_entity_access("User")
            .expect("entity access");
        let key = step
            .internal_entity_key_schema()
            .encode_entity(&[
                CanonicalValue::Uuid([0x11; 16]),
                CanonicalValue::Uuid([0x22; 16]),
            ])
            .expect("entity key");
        let output = CanonicalRecord::new(vec![
            (
                access.internal_field_id("user_id").expect("user id"),
                CanonicalValue::Uuid([0x22; 16]),
            ),
            (
                access.internal_field_id("name").expect("name"),
                CanonicalValue::string("Ada").expect("string"),
            ),
        ])
        .expect("output");
        let epoch = CommitSequence::new(17).expect("epoch");
        let observed = ExactTextProjectionResult::new(
            vec![ExactTextProjectionRow::new(key, output)],
            3,
            epoch,
            ProjectionGeneration::first(),
            exact.binding().plan().provider_digest(),
            9,
        );
        let result = exact_result_response(
            exact,
            query.shared_document().as_ref(),
            module.identity(),
            observed,
            Arc::new(BTreeMap::new()),
        )
        .expect("response");

        assert_eq!(result.application_head(), epoch.get());
        assert_eq!(result.outcome(), "Found");
        let SymbolicResultField::Many(users) = &result.fields()["users"] else {
            panic!("users list");
        };
        assert_eq!(users.len(), 1);
        assert_eq!(users[0].entity(), "User");
        assert_eq!(
            users[0].fields()["name"],
            CanonicalValue::string("Ada").unwrap()
        );
        let SymbolicResultField::One(total) = &result.fields()["total"] else {
            panic!("total record");
        };
        assert_eq!(total.fields()["value"], CanonicalValue::U64(3));
        assert!(result.next_cursor().is_none());
        assert_eq!(result.identity().plan_hash(), exact.identity());
    }

    #[test]
    fn exact_defaults_are_materialized_before_authorization_and_provider_work() {
        let document = parse_query(QUERY).expect("query");
        let submitted = SymbolicQueryParameters::new(BTreeMap::new()).expect("parameters");
        let materialized =
            exact_parameters_with_defaults(&document, submitted, ["limit", "offset"])
                .expect("defaults");
        assert_eq!(materialized.get("limit"), Some(&SubmittedValue::U64(50)));
        assert_eq!(materialized.get("offset"), Some(&SubmittedValue::U64(0)));
    }

    #[test]
    fn exact_provider_lifecycle_has_one_closed_public_mapping() {
        let mappings = [
            (
                ExactTextProjectionPortError::Building,
                Some(ApplicationErrorCode::QueryUnavailable),
            ),
            (
                ExactTextProjectionPortError::Rebuilding,
                Some(ApplicationErrorCode::QueryUnavailable),
            ),
            (
                ExactTextProjectionPortError::Unavailable,
                Some(ApplicationErrorCode::QueryUnavailable),
            ),
            (
                ExactTextProjectionPortError::SnapshotRetired,
                Some(ApplicationErrorCode::SnapshotRetired),
            ),
            (
                ExactTextProjectionPortError::FreshnessUnsatisfied,
                Some(ApplicationErrorCode::FreshnessUnsatisfied),
            ),
            (
                ExactTextProjectionPortError::Diverged,
                Some(ApplicationErrorCode::ProjectionDiverged),
            ),
            (ExactTextProjectionPortError::Integrity, None),
        ];
        for (error, expected) in mappings {
            assert_eq!(exact_projection_application_code(error), expected);
        }
    }

    #[test]
    fn exact_predicate_page_and_count_are_shaped_from_one_sealed_observation() {
        let bundle =
            riffdb_contract_compiler::compile_contract_source(RICH_CONTRACT).expect("contract");
        let module = QueryModule::compile(
            QueryModuleCandidate::new(
                QueryModuleName::new("directory").expect("module name"),
                QueryModuleVersion::new(1).expect("module version"),
                vec![NamedQuerySource::new("SearchUsers", RICH_QUERY).expect("query")],
            )
            .expect("candidate"),
            &bundle,
        )
        .expect("module");
        let query = module.query("SearchUsers").expect("query");
        let exact = query.exact_predicate_result().expect("V6 exact plan");
        let program = exact.representative_program();
        let step = &program.steps()[0];
        let access = program.internal_entity_access("User").expect("access");
        let key = step
            .internal_entity_key_schema()
            .encode_entity(&[
                CanonicalValue::Uuid([0x11; 16]),
                CanonicalValue::Uuid([0x22; 16]),
            ])
            .expect("key");
        let reviewed = riffdb_types::Timestamp::new(1_700_000_000, 0).expect("timestamp");
        let output = CanonicalRecord::new(vec![
            (
                access.internal_field_id("user_id").expect("user id"),
                CanonicalValue::Uuid([0x22; 16]),
            ),
            (
                access.internal_field_id("email").expect("email"),
                CanonicalValue::string("ada@example.test").expect("email"),
            ),
            (
                access.internal_field_id("state").expect("state"),
                CanonicalValue::string("active").expect("state"),
            ),
            (
                access.internal_field_id("created_at").expect("created"),
                CanonicalValue::U64(40),
            ),
            (
                access.internal_field_id("reviewed_at").expect("reviewed"),
                CanonicalValue::Timestamp(reviewed),
            ),
        ])
        .expect("output");
        let epoch = CommitSequence::new(29).expect("epoch");
        let observed = ExactPredicateProjectionResult::new(
            vec![ExactTextProjectionRow::new(key, output)],
            7,
            epoch,
            ProjectionGeneration::first(),
            exact.program().provider_descriptor().unwrap().digest(),
            3,
        );
        let exact = ExactPredicateNamedQuery::PresentOnly(
            query
                .shared_exact_predicate_result()
                .expect("shared exact plan"),
        );
        let result = exact_predicate_result_response(
            &exact,
            query.shared_document().as_ref(),
            module.identity(),
            observed,
            Arc::new(BTreeMap::new()),
        )
        .expect("response");
        assert_eq!(result.application_head(), 29);
        let SymbolicResultField::Many(users) = &result.fields()["users"] else {
            panic!("users");
        };
        assert_eq!(users.len(), 1);
        let SymbolicResultField::One(total) = &result.fields()["total"] else {
            panic!("total");
        };
        assert_eq!(total.fields()["value"], CanonicalValue::U64(7));
        assert_eq!(result.identity().plan_hash(), exact.identity());
    }

    #[test]
    fn exact_set_parameters_are_canonical_and_bounded_before_provider_work() {
        let duplicate = CanonicalValue::string("disabled").expect("string");
        let values = CanonicalValue::list(vec![duplicate.clone(), duplicate]).expect("list");
        let ExactParameterValueV1::Set(values) = exact_parameter_value(&values).expect("set")
        else {
            panic!("set");
        };
        assert_eq!(values, vec![ExactScalarV1::String("disabled".to_owned())]);
        let excessive = CanonicalValue::list(
            (0..=riffdb_query_ir::MAX_EXACT_SET_VALUES_V1)
                .map(|value| CanonicalValue::U64(value as u64))
                .collect(),
        )
        .expect("list");
        assert!(exact_parameter_value(&excessive).is_none());
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

#[cfg(test)]
mod enum_map_sharing_tests {
    use super::*;

    #[test]
    fn enum_variant_names_are_shared_by_bundle_publication() {
        let source = r#"
contract EnumShare version 1 {
  enum Status { Open, Closed }
  entity Item {
    key (id: uuid)
    field status: Status
  }
  aggregate Items {
    root Item
    partition_by id
    conflict_key (id)
  }
  command Seed {
    input idempotency_key: string<128>
    input id: uuid
    idempotency_key idempotency_key
    create Item(id) as item else Exists { id: id }
    set item.status = Status.Open
    return Created { item: item }
  }
}

"#;
        let compiled = riffdb_contract_compiler::compile_contract_source(source).expect("contract");
        let first = riffdb_catalog::ValidatedContractBundle::from_compiler_bundle(compiled.clone())
            .expect("validated");
        // Same publication bytes re-decoded produce distinct Arc shells, but
        // cloning one validated bundle shares the table pointer.
        let second = first.clone();
        assert!(
            Arc::ptr_eq(first.enum_variant_names(), second.enum_variant_names()),
            "clones of one validated publication must share one enum-name Arc"
        );
        // Re-decode of the same bytes is a separate publication object; tables
        // are equal by content, bounded by the catalog publication lifetime.
        let redecoded = riffdb_catalog::ValidatedContractBundle::from_compiler_bundle(compiled)
            .expect("revalidated");
        assert_eq!(
            first.enum_variant_names().as_ref(),
            redecoded.enum_variant_names().as_ref()
        );
        assert!(first.enum_variant_names().contains_key(&(
            first.bundle().schema().enums()[0].id().get(),
            first.bundle().schema().enums()[0].variants()[0].id().get()
        )));
    }
}

#[cfg(test)]
mod reimport_observation_tests {
    use super::*;

    fn expected_hash(value: &str) -> riffdb_types::GeneratedArtifactHash {
        let mut bytes = [0_u8; 32];
        for (index, pair) in value.as_bytes().chunks_exact(2).enumerate() {
            let digit = |value: u8| match value {
                b'0'..=b'9' => value - b'0',
                b'a'..=b'f' => value - b'a' + 10,
                _ => panic!("invalid fixture hash"),
            };
            bytes[index] = (digit(pair[0]) << 4) | digit(pair[1]);
        }
        riffdb_types::GeneratedArtifactHash::from_bytes(bytes)
    }

    fn result(head: u64, title: &str) -> ExecuteSymbolicQueryResult {
        let record = SymbolicResultRecord {
            entity: Arc::from("Ticket"),
            fields: BTreeMap::from([
                (Arc::from("ticket_id"), CanonicalValue::Uuid([0x11; 16])),
                (
                    Arc::from("title"),
                    CanonicalValue::string(title).expect("title"),
                ),
            ]),
            nested: BTreeMap::new(),
            exact_decimals: BTreeMap::new(),
            exact_means: BTreeMap::new(),
        };
        ExecuteSymbolicQueryResult {
            identity: SymbolicQueryIdentity {
                lineage: ContractLineage::new("TicketDesk").expect("lineage"),
                version: ContractVersion::new(1).expect("version"),
                bundle_hash: ContractBundleHash::from_bytes([0x22; 32]),
                name: Some("GetTicket".to_owned()),
                plan_hash: QueryPlanHash::from_bytes([0x33; 32]),
                module_hash: Some(QueryModuleHash::from_bytes([0x44; 32])),
            },
            outcome: "Found".to_owned(),
            application_head: head,
            fields: BTreeMap::from([("ticket".to_owned(), SymbolicResultField::One(record))]),
            compact_result: None,
            packed_result: false,
            enum_variant_names: Arc::new(BTreeMap::new()),
            next_cursor: None,
        }
    }

    #[test]
    fn reconciliation_digest_excludes_physical_frontier_but_includes_semantic_values() {
        let first = hash_reimport_query_result(&result(10, "Cannot sign in")).expect("hash");
        let same_state =
            hash_reimport_query_result(&result(999, "Cannot sign in")).expect("same hash");
        let changed =
            hash_reimport_query_result(&result(10, "Cannot reset password")).expect("changed hash");
        assert_eq!(first, same_state);
        assert_ne!(first, changed);
    }

    #[test]
    fn empty_reconciliation_shapes_match_portability_fixture_digests() {
        let mut missing = result(1, "unused");
        missing.outcome = "Missing".to_owned();
        missing.fields.clear();
        assert_eq!(
            hash_reimport_query_result(&missing).expect("missing hash"),
            expected_hash("cc9fc643cfeb58b06e79e478e37d05aebfde348589c64d94a991daf878cbea9d")
        );

        let mut collection = result(1, "unused");
        collection.fields =
            BTreeMap::from([("tuples".to_owned(), SymbolicResultField::Many(Vec::new()))]);
        assert_eq!(
            hash_reimport_query_result(&collection).expect("collection hash"),
            expected_hash("a9b45a1bfe185d2cd91261ef6544a53eaf9fc020c7c2edc096159db5ea09969a")
        );
    }
}
