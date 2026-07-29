//! Symbolic RiffQL application operations over the shared service boundary.

use std::collections::BTreeMap;
use std::num::NonZeroU16;
use std::sync::Arc;

use riffdb_contract_ir::{ValueType, ValueTypeTag};
use riffdb_errors::{
    PublicError, ValidationCode, ValidationIssue, ValidationIssues, ValidationPath,
};
use riffdb_policy::{
    Decision, OperationRequest, OperationTenantScope, OutputClassification, PartitionConstraint,
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
use riffdb_riffql_syntax::{Document, ParseDiagnostic, Span, TypeReference, parse_query};
use riffdb_types::{
    CanonicalValue, ContractBundleHash, ContractLineage, ContractVersion, QueryPlanHash,
    ScopedPartitionV1, ServiceOperationV1, TenantScope, encode_canonical_value,
    hash_query_parameters,
};

use crate::command_operations::{SubmittedValueMaterializationError, materialize_submitted_value};
use crate::orchestration::AuditScope;
use crate::query_discovery_operations::{
    finish_failure, finish_success, prepare_selected_contract,
};
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
}

impl SymbolicQueryIdentity {
    fn from_program(program: &QueryAccessProgramV1) -> Self {
        Self {
            lineage: program.contract().lineage().clone(),
            version: program.contract().version(),
            bundle_hash: program.contract().bundle_hash(),
            name: program.name().map(str::to_owned),
            plan_hash: program.identity().hash(),
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
    Valid(CheckedSymbolicQuery),
    /// Bounded compiler diagnostics; no partial program escaped.
    Invalid(Vec<SymbolicDiagnostic>),
}

/// Query-explain response.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ExplainSymbolicQueryResult {
    /// Fully checked identity, schema, and name-only plan.
    Valid {
        /// Query descriptor.
        query: CheckedSymbolicQuery,
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

/// One ad-hoc symbolic execution request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExecuteSymbolicQueryRequest {
    contract: SymbolicContractSelector,
    source: SymbolicQuerySource,
    parameters: SymbolicQueryParameters,
    cursor: Option<CursorToken>,
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
        }
    }

    /// Attaches one opaque continuation token.
    #[must_use]
    pub const fn with_cursor(mut self, cursor: CursorToken) -> Self {
        self.cursor = Some(cursor);
        self
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
    next_cursor: Option<CursorToken>,
}

impl ExecuteSymbolicQueryResult {
    fn from_snapshot(program: &QueryAccessProgramV1, snapshot: &QueryOwnedSnapshot) -> Self {
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
        catalog: render_catalog(&catalog),
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
        OperationRequest::check_query(),
        OPERATION,
    )
    .await?;
    let result = match compile(request.source().as_str(), bundle.bundle()) {
        Ok(program) => {
            CheckSymbolicQueryResult::Valid(CheckedSymbolicQuery::from_program(&program))
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
        OperationRequest::explain_query(),
        OPERATION,
    )
    .await?;
    let result = match compile(request.source().as_str(), bundle.bundle()) {
        Ok(program) => ExplainSymbolicQueryResult::Valid {
            query: CheckedSymbolicQuery::from_program(&program),
            lines: program.explain().lines().to_vec(),
        },
        Err(diagnostics) => ExplainSymbolicQueryResult::Invalid(diagnostics),
    };
    begun.reauthorize(&service, &context).await?;
    finish_success(&service, &context, &begun).await?;
    Ok(result)
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
    let compiled = compile_parts(request.source().as_str(), bundle.bundle())
        .map_err(|_| validation_failure(ValidationCode::InvalidValue))?;
    let program = compiled.program;
    if program.steps().len() > MAX_SYMBOLIC_QUERY_STEPS {
        return Err(validation_failure(ValidationCode::InvalidValue));
    }
    let parameters = materialize_query_parameters(
        &service,
        OPERATION,
        bundle.bundle(),
        &compiled.document,
        request.parameters(),
    )?;
    let parameter_hash = query_parameter_hash(&parameters)
        .ok_or_else(|| validation_failure(ValidationCode::InvalidValue))?;
    let cursor_lookup = QueryCursorLookup::new(
        CursorContractIdentity::new(
            program.contract().lineage().clone(),
            program.contract().version(),
            program.contract().bundle_hash(),
        ),
        program.identity().hash(),
        parameter_hash,
        context.principal().capability_id(),
        context.principal().capability_revision(),
    );
    let begun = begin_symbolic(
        &service,
        &context,
        &bundle,
        OperationRequest::execute_query(),
        OPERATION,
    )
    .await?;
    if !authorize_program(&service, &context, bundle.bundle(), &program, &parameters) {
        return Err(begun.finish_authorization_denial(&service, &context).await);
    }
    let prior = match request.cursor() {
        Some(token) => match service.cursors.resolve_query(
            token,
            context.principal().principal_id(),
            &cursor_lookup,
        ) {
            Ok(state) => Some(state),
            Err(CursorAccessError::InvalidCursor) => {
                let failure = validation_failure(ValidationCode::InvalidValue);
                return Err(finish_failure(&service, &context, &begun, failure).await);
            }
            Err(CursorAccessError::Unavailable) => {
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
    let snapshot = match executor.execute_query_page(
        &program,
        &parameters,
        prior.as_deref().map(QueryCursorState::continuation),
    ) {
        Ok(snapshot) => snapshot,
        Err(error) => {
            let failure = execution_failure(&service, OPERATION, error);
            return Err(finish_failure(&service, &context, &begun, failure).await);
        }
    };
    begun.reauthorize(&service, &context).await?;
    if !authorize_program(&service, &context, bundle.bundle(), &program, &parameters) {
        return Err(begun.finish_authorization_denial(&service, &context).await);
    }
    let continuation = match (snapshot.continuation_binding(), snapshot.continuation()) {
        (Some(binding), Some(lower)) => QueryContinuation::checked(
            binding.to_owned(),
            lower.to_vec(),
            snapshot.index_epochs().clone(),
        ),
        (None, None) => None,
        _ => {
            let failure = service.internal_failure(OPERATION, InternalDefect::ProofMismatch);
            return Err(finish_failure(&service, &context, &begun, failure).await);
        }
    };
    let cursor_guard = match continuation {
        Some(continuation) => match service.cursors.register_query_unpublished(
            context.principal().principal_id(),
            cursor_lookup,
            QueryCursorState::new(continuation),
        ) {
            Ok(guard) => Some(guard),
            Err(_) => {
                let failure = PublicError::storage_unavailable().into();
                return Err(finish_failure(&service, &context, &begun, failure).await);
            }
        },
        None => None,
    };
    let mut result = ExecuteSymbolicQueryResult::from_snapshot(&program, &snapshot);
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
    let coerced = coerce_natural_query_value(value_type, submitted)
        .map_err(|code| validation_failure(code))?;
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
        (ValueTypeTag::Uuid, SubmittedValue::String(value)) => parse_uuid(value.as_str())
            .map(SubmittedValue::Uuid)
            .ok_or(ValidationCode::InvalidValue),
        (ValueTypeTag::I64, SubmittedValue::U64(value)) => i64::try_from(*value)
            .map(SubmittedValue::I64)
            .map_err(|_| ValidationCode::InvalidValue),
        (ValueTypeTag::U64, SubmittedValue::I64(value)) => u64::try_from(*value)
            .map(SubmittedValue::U64)
            .map_err(|_| ValidationCode::InvalidValue),
        _ => Ok(submitted.clone()),
    }
}

fn parse_uuid(text: &str) -> Option<[u8; 16]> {
    if text.len() != 36
        || text.as_bytes().get(8) != Some(&b'-')
        || text.as_bytes().get(13) != Some(&b'-')
        || text.as_bytes().get(18) != Some(&b'-')
        || text.as_bytes().get(23) != Some(&b'-')
    {
        return None;
    }
    let mut bytes = [0_u8; 16];
    let mut output = 0;
    let mut high = None;
    for byte in text.bytes() {
        if byte == b'-' {
            continue;
        }
        let nibble = match byte {
            b'0'..=b'9' => byte - b'0',
            b'a'..=b'f' => byte - b'a' + 10,
            _ => return None,
        };
        if let Some(high) = high.take() {
            *bytes.get_mut(output)? = high << 4 | nibble;
            output += 1;
        } else {
            high = Some(nibble);
        }
    }
    (output == bytes.len() && high.is_none()).then_some(bytes)
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

fn authorize_program(
    service: &RiffDbServiceInner,
    context: &RequestContext,
    bundle: &riffdb_contract_ir::ContractBundle,
    program: &QueryAccessProgramV1,
    parameters: &QueryParameters,
) -> bool {
    let Some(partition_value) = parameters.get(program.partition_parameter()) else {
        return false;
    };
    let lineage = program.contract().lineage();
    for step in program.steps() {
        let Some(entity) = program
            .authorization()
            .iter()
            .find(|access| access.entity() == step.entity())
        else {
            return false;
        };
        let Some(contract_entity) = bundle.schema().entity(step.internal_entity_id()) else {
            return false;
        };
        let mut non_key_fields = entity
            .internal_fields()
            .filter_map(|(_, field)| {
                (!contract_entity.primary_key_fields().contains(&field)).then_some(field)
            })
            .collect::<Vec<_>>();
        non_key_fields.sort_unstable();
        non_key_fields.dedup();
        let partition = {
            let Some(aggregate) = bundle
                .schema()
                .aggregate_for_entity(step.internal_entity_id())
            else {
                return false;
            };
            let Ok(partition) = aggregate
                .keys()
                .partition_schema()
                .encode_partition(std::slice::from_ref(partition_value))
            else {
                return false;
            };
            partition
        };
        let request = match step.access() {
            QueryAccessKind::Point { .. } => OperationRequest::get_entity(
                lineage.clone(),
                program.contract().version(),
                step.internal_entity_id(),
                OperationTenantScope::global_only(),
                partition.clone(),
                non_key_fields.clone(),
            ),
            QueryAccessKind::Index { .. } => {
                let Some(index_id) = step.internal_index_id() else {
                    return false;
                };
                let Ok(rows) = u16::try_from(step.maximum_rows()) else {
                    return false;
                };
                let Some(rows) = NonZeroU16::new(rows) else {
                    return false;
                };
                OperationRequest::scan_index(
                    lineage.clone(),
                    program.contract().version(),
                    index_id,
                    step.internal_entity_id(),
                    OperationTenantScope::global_only(),
                    non_key_fields.clone(),
                    rows,
                )
            }
        };
        let Ok(request) = request else {
            return false;
        };
        let Ok(Decision::Allow(authorization)) = service
            .providers
            .policy
            .authorize(context.principal(), request)
        else {
            return false;
        };
        let obligations = authorization.obligations();
        let mask_matches = obligations.field_mask().is_some_and(|mask| {
            mask.lineage() == lineage
                && mask.entity_type_id() == step.internal_entity_id()
                && mask.fields() == non_key_fields
        });
        let common = authorization.database_id() == service.identity.database_id()
            && authorization.environment() == service.identity.environment()
            && obligations.effective_tenant_scope() == &TenantScope::Global
            && obligations.output_classification()
                == OutputClassification::PolicyFilteredApplicationData
            && mask_matches;
        let access_matches = match step.access() {
            QueryAccessKind::Point { .. } => {
                authorization.operation() == ServiceOperationV1::GetEntity
                    && obligations.row_limit().is_none()
                    && obligations.partition_constraint()
                        == Some(&PartitionConstraint::Exact(ScopedPartitionV1::new(
                            lineage.clone(),
                            partition,
                        )))
            }
            QueryAccessKind::Index { .. } => {
                authorization.operation() == ServiceOperationV1::ScanIndex
                    && obligations
                        .row_limit()
                        .is_some_and(|limit| u64::from(limit.get()) >= step.maximum_rows())
                    && obligations
                        .partition_constraint()
                        .is_some_and(|constraint| {
                            partition_allowed(constraint, lineage, &partition)
                        })
            }
        };
        if !common || !access_matches {
            return false;
        }
    }
    true
}

fn partition_allowed(
    constraint: &PartitionConstraint,
    lineage: &ContractLineage,
    partition: &riffdb_types::PartitionKey,
) -> bool {
    let scoped = ScopedPartitionV1::new(lineage.clone(), partition.clone());
    match constraint {
        PartitionConstraint::Exact(expected) => expected == &scoped,
        PartitionConstraint::Filter(riffdb_types::PartitionScopeV1::All) => true,
        PartitionConstraint::Filter(riffdb_types::PartitionScopeV1::Explicit(entries)) => {
            entries.iter().any(|candidate| candidate == &scoped)
        }
    }
}

fn execution_failure(
    service: &RiffDbServiceInner,
    operation: ServiceOperationV1,
    error: QueryExecutionError,
) -> ServiceFailure {
    match error {
        QueryExecutionError::MissingParameter { .. }
        | QueryExecutionError::InvalidParameter { .. }
        | QueryExecutionError::StaleCursor
        | QueryExecutionError::InvalidContinuation => {
            validation_failure(ValidationCode::InvalidValue)
        }
        QueryExecutionError::BackendUnavailable => PublicError::storage_unavailable().into(),
        QueryExecutionError::BoundExceeded => ServiceFailure::ResponseTooLarge,
        QueryExecutionError::MissingField { .. }
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

fn render_catalog(catalog: &SymbolicCatalog) -> String {
    let mut lines = Vec::new();
    for enumeration in catalog.enums() {
        lines.push(format!(
            "enum {} {{ {} }}",
            enumeration.name(),
            enumeration.variants().collect::<Vec<_>>().join(", ")
        ));
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
