#![forbid(unsafe_code)]

//! Transport-neutral, public-safe errors for RiffDB.
//!
//! Declared business outcomes are contract values and intentionally do not
//! appear in this crate. [`PublicError`] describes failures outside that
//! outcome algebra without retaining untrusted diagnostic text or internal
//! error sources.

use std::error::Error;
use std::fmt;

use riffdb_types::{
    ContractLineage, ContractVersion, ExecutionFailureCode, FieldId, IncidentId, RequestId,
};

/// Version of the bounded application-semantic error envelope.
pub const APPLICATION_ERROR_ENVELOPE_VERSION: u32 = 1;
/// Maximum encoded application-error details accepted at a public boundary.
pub const MAX_APPLICATION_ERROR_BYTES: usize = 16 * 1024;
/// Maximum number of symbolic path segments in an application error.
pub const MAX_APPLICATION_SYMBOL_PATH_SEGMENTS: usize = 16;
/// Maximum bytes in one application operation or path symbol.
pub const MAX_APPLICATION_SYMBOL_BYTES: usize = 256;
/// Maximum number of closed remediation codes in one application error.
pub const MAX_APPLICATION_FIXES: usize = 8;
/// Maximum RiffQL source offset retained in a public error.
pub const MAX_APPLICATION_SOURCE_OFFSET: u64 = 262_144;

/// Closed application operation inventory used only for public-safe context.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum ApplicationOperation {
    /// Describe a symbolic contract catalog.
    DescribeContract,
    /// Check RiffQL source.
    CheckQuery,
    /// Explain ad-hoc or named RiffQL.
    ExplainQuery,
    /// Execute ad-hoc or named RiffQL.
    ExecuteQuery,
    /// Compile and deploy a named-query module.
    DeployQueryModule,
    /// Compile and publish an immutable reactive module.
    DeployReactiveModule,
    /// Inspect a named-query module.
    GetQueryModule,
    /// Execute a symbolic command.
    ExecuteCommand,
    /// Execute a bounded command batch.
    BatchCommand,
    /// Execute one projected columnar query under a freshness policy.
    ExecuteProjectedQuery,
}

impl ApplicationOperation {
    /// Returns the stable application-facing operation name.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::DescribeContract => "DescribeContract",
            Self::CheckQuery => "CheckQuery",
            Self::ExplainQuery => "ExplainQuery",
            Self::ExecuteQuery => "ExecuteQuery",
            Self::DeployQueryModule => "DeployQueryModule",
            Self::DeployReactiveModule => "DeployReactiveModule",
            Self::GetQueryModule => "GetQueryModule",
            Self::ExecuteCommand => "ExecuteCommand",
            Self::BatchCommand => "BatchCommand",
            Self::ExecuteProjectedQuery => "ExecuteProjectedQuery",
        }
    }
}

/// Closed stable code registry for application-semantic failures.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum ApplicationErrorCode {
    /// Public message bytes failed structural validation.
    InvalidRequest,
    /// A typed application input failed validation.
    InputInvalid,
    /// Current policy denied the operation.
    AuthorizationDenied,
    /// Contract identity is absent, stale, or does not match.
    ContractMismatch,
    /// RiffQL could not be parsed, resolved, typed, or planned.
    QueryInvalid,
    /// A checked query cannot currently be loaded or executed.
    QueryUnavailable,
    /// The requested named-query module is absent or stale.
    ModuleUnavailable,
    /// A continuation cursor is invalid or stale.
    CursorInvalid,
    /// One complete authorized result exceeds a public bound.
    ResponseTooLarge,
    /// Authoritative storage is temporarily unavailable.
    StorageUnavailable,
    /// A durable mutation may have completed.
    OutcomeUnknown,
    /// The request was cancelled at a safe point.
    OperationCancelled,
    /// The request deadline elapsed.
    DeadlineExceeded,
    /// A redacted internal defect occurred.
    InternalDefect,
    /// An idempotency identity was reused with different input.
    IdempotencyKeyReuse,
    /// Deterministic command execution failed.
    CommandExecutionFailed,
    /// A formerly valid capability is revoked.
    CapabilityRevoked,
    /// A public peer violated the application protocol.
    ProtocolInvalid,
    /// Observed history predates a database restore.
    HistoryIncarnationMismatch,
    /// Requested history was retired by retention prune.
    HistoryPruned,
    /// The service is over capacity and rejected admission.
    Overloaded,
}

/// Complete v1 application error code registry in stable wire order.
pub const APPLICATION_ERROR_CODES: [ApplicationErrorCode; 21] = [
    ApplicationErrorCode::InvalidRequest,
    ApplicationErrorCode::InputInvalid,
    ApplicationErrorCode::AuthorizationDenied,
    ApplicationErrorCode::ContractMismatch,
    ApplicationErrorCode::QueryInvalid,
    ApplicationErrorCode::QueryUnavailable,
    ApplicationErrorCode::ModuleUnavailable,
    ApplicationErrorCode::CursorInvalid,
    ApplicationErrorCode::ResponseTooLarge,
    ApplicationErrorCode::StorageUnavailable,
    ApplicationErrorCode::OutcomeUnknown,
    ApplicationErrorCode::OperationCancelled,
    ApplicationErrorCode::DeadlineExceeded,
    ApplicationErrorCode::InternalDefect,
    ApplicationErrorCode::IdempotencyKeyReuse,
    ApplicationErrorCode::CommandExecutionFailed,
    ApplicationErrorCode::CapabilityRevoked,
    ApplicationErrorCode::ProtocolInvalid,
    ApplicationErrorCode::HistoryIncarnationMismatch,
    ApplicationErrorCode::HistoryPruned,
    ApplicationErrorCode::Overloaded,
];

impl ApplicationErrorCode {
    /// Maps the compatible kernel classification into the application registry.
    #[must_use]
    pub const fn from_public_kind(kind: PublicErrorKind) -> Self {
        match kind {
            PublicErrorKind::Validation => Self::InputInvalid,
            PublicErrorKind::IdempotencyKeyReuse => Self::IdempotencyKeyReuse,
            PublicErrorKind::AuthorizationDenied => Self::AuthorizationDenied,
            PublicErrorKind::ConcurrencyDeadlineExceeded => Self::DeadlineExceeded,
            PublicErrorKind::ContractMismatch => Self::ContractMismatch,
            PublicErrorKind::StorageUnavailable => Self::StorageUnavailable,
            PublicErrorKind::OutcomeUnknown => Self::OutcomeUnknown,
            PublicErrorKind::InternalDefect => Self::InternalDefect,
            PublicErrorKind::CommandExecutionFailed => Self::CommandExecutionFailed,
            PublicErrorKind::HistoryIncarnationMismatch => Self::HistoryIncarnationMismatch,
            PublicErrorKind::HistoryPruned => Self::HistoryPruned,
            PublicErrorKind::Overloaded => Self::Overloaded,
        }
    }

    /// Returns the stable externally documented code.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::InvalidRequest => "RDB-APP-0001",
            Self::InputInvalid => "RDB-INPUT-0101",
            Self::AuthorizationDenied => "RDB-AUTH-0214",
            Self::ContractMismatch => "RDB-CONTRACT-0101",
            Self::QueryInvalid => "RDB-QUERY-0101",
            Self::QueryUnavailable => "RDB-QUERY-0102",
            Self::ModuleUnavailable => "RDB-MODULE-0101",
            Self::CursorInvalid => "RDB-CURSOR-0101",
            Self::ResponseTooLarge => "RDB-RESOURCE-0101",
            Self::StorageUnavailable => "RDB-STORAGE-0101",
            Self::OutcomeUnknown => "RDB-UNCERTAIN-0101",
            Self::OperationCancelled => "RDB-APP-0002",
            Self::DeadlineExceeded => "RDB-APP-0003",
            Self::InternalDefect => "RDB-INTERNAL-0001",
            Self::IdempotencyKeyReuse => "RDB-COMMAND-0101",
            Self::CommandExecutionFailed => "RDB-COMMAND-0102",
            Self::CapabilityRevoked => "RDB-AUTH-0215",
            Self::ProtocolInvalid => "RDB-PROTOCOL-0101",
            Self::HistoryIncarnationMismatch => "RDB-HISTORY-0101",
            Self::HistoryPruned => "RDB-HISTORY-0102",
            Self::Overloaded => "RDB-CAPACITY-0101",
        }
    }

    /// Returns the one registry-owned public message for this code.
    #[must_use]
    pub const fn safe_message(self) -> &'static str {
        match self {
            Self::InvalidRequest => "application request is structurally invalid",
            Self::InputInvalid => "application input is invalid",
            Self::AuthorizationDenied => "application operation is not authorized",
            Self::ContractMismatch => "application contract does not match",
            Self::QueryInvalid => "RiffQL query is invalid",
            Self::QueryUnavailable => "RiffQL query is unavailable",
            Self::ModuleUnavailable => "query module is unavailable",
            Self::CursorInvalid => "query cursor is invalid or stale",
            Self::ResponseTooLarge => "application result exceeds the service limit",
            Self::StorageUnavailable => "storage is temporarily unavailable",
            Self::OutcomeUnknown => "command outcome is not yet known",
            Self::OperationCancelled => "application request was cancelled",
            Self::DeadlineExceeded => "application request deadline elapsed",
            Self::InternalDefect => "an internal error occurred",
            Self::IdempotencyKeyReuse => {
                "idempotency key was reused with different application input"
            }
            Self::CommandExecutionFailed => "command execution failed",
            Self::CapabilityRevoked => "application capability is revoked",
            Self::ProtocolInvalid => "the RiffDB peer returned an invalid application response",
            Self::HistoryIncarnationMismatch => "observed history predates a database restore",
            Self::HistoryPruned => "requested history has been pruned",
            Self::Overloaded => "service is over capacity",
        }
    }

    /// Returns the closed category.
    #[must_use]
    pub const fn category(self) -> ApplicationErrorCategory {
        match self {
            Self::InvalidRequest | Self::InputInvalid => ApplicationErrorCategory::Input,
            Self::AuthorizationDenied | Self::CapabilityRevoked => {
                ApplicationErrorCategory::Authorization
            }
            Self::ContractMismatch => ApplicationErrorCategory::Contract,
            Self::QueryInvalid | Self::QueryUnavailable => ApplicationErrorCategory::Query,
            Self::ModuleUnavailable => ApplicationErrorCategory::Module,
            Self::CursorInvalid => ApplicationErrorCategory::Cursor,
            Self::ResponseTooLarge => ApplicationErrorCategory::Resource,
            Self::StorageUnavailable => ApplicationErrorCategory::Storage,
            Self::OutcomeUnknown => ApplicationErrorCategory::Uncertainty,
            Self::OperationCancelled | Self::DeadlineExceeded => ApplicationErrorCategory::Control,
            Self::InternalDefect => ApplicationErrorCategory::Internal,
            Self::IdempotencyKeyReuse | Self::CommandExecutionFailed => {
                ApplicationErrorCategory::Command
            }
            Self::ProtocolInvalid => ApplicationErrorCategory::Protocol,
            Self::HistoryIncarnationMismatch | Self::HistoryPruned => {
                ApplicationErrorCategory::History
            }
            Self::Overloaded => ApplicationErrorCategory::Capacity,
        }
    }

    /// Returns deterministic recovery guidance.
    #[must_use]
    pub const fn recovery_action(self) -> ApplicationRecoveryAction {
        match self {
            Self::InvalidRequest
            | Self::InputInvalid
            | Self::QueryInvalid
            | Self::CursorInvalid
            | Self::ResponseTooLarge
            | Self::IdempotencyKeyReuse
            | Self::HistoryIncarnationMismatch
            | Self::HistoryPruned => ApplicationRecoveryAction::CorrectRequest,
            Self::AuthorizationDenied | Self::CapabilityRevoked => {
                ApplicationRecoveryAction::ObtainPermission
            }
            Self::ContractMismatch | Self::QueryUnavailable | Self::ModuleUnavailable => {
                ApplicationRecoveryAction::RefreshContract
            }
            Self::StorageUnavailable | Self::DeadlineExceeded | Self::Overloaded => {
                ApplicationRecoveryAction::Retry
            }
            Self::OutcomeUnknown => ApplicationRecoveryAction::ResolveWithSameIdempotencyKey,
            Self::OperationCancelled => ApplicationRecoveryAction::None,
            Self::InternalDefect | Self::CommandExecutionFailed | Self::ProtocolInvalid => {
                ApplicationRecoveryAction::ContactOperator
            }
        }
    }

    /// Returns the exact deterministic remediation set.
    #[must_use]
    pub const fn fixes(self) -> &'static [ApplicationFixCode] {
        match self {
            Self::InvalidRequest
            | Self::InputInvalid
            | Self::QueryInvalid
            | Self::HistoryIncarnationMismatch
            | Self::HistoryPruned => &[ApplicationFixCode::CorrectInput],
            Self::AuthorizationDenied | Self::CapabilityRevoked => {
                &[ApplicationFixCode::BindApplicationRole]
            }
            Self::ContractMismatch => &[ApplicationFixCode::RefreshContract],
            Self::QueryUnavailable | Self::ModuleUnavailable => {
                &[ApplicationFixCode::PinActiveModule]
            }
            Self::CursorInvalid => &[ApplicationFixCode::RestartFromFirstPage],
            Self::ResponseTooLarge | Self::IdempotencyKeyReuse => {
                &[ApplicationFixCode::CorrectInput]
            }
            Self::StorageUnavailable | Self::DeadlineExceeded | Self::Overloaded => {
                &[ApplicationFixCode::RetryLater]
            }
            Self::OutcomeUnknown => &[ApplicationFixCode::ResolveWithSameIdempotencyKey],
            Self::InternalDefect => &[ApplicationFixCode::ContactOperatorWithIncident],
            Self::OperationCancelled | Self::CommandExecutionFailed | Self::ProtocolInvalid => &[],
        }
    }
}

/// Coarse application failure category.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum ApplicationErrorCategory {
    /// Caller-controlled application input.
    Input,
    /// Authentication, role, or authorization policy.
    Authorization,
    /// Contract selection and compatibility.
    Contract,
    /// RiffQL compilation or execution.
    Query,
    /// Named-query module selection.
    Module,
    /// Cursor validation.
    Cursor,
    /// Public resource bounds.
    Resource,
    /// Authoritative storage availability.
    Storage,
    /// Mutation uncertainty.
    Uncertainty,
    /// Cancellation, deadline, or redacted defects.
    Internal,
    /// Peer protocol conformance.
    Protocol,
    /// Symbolic command admission or execution.
    Command,
    /// Cancellation and deadline control.
    Control,
    /// History incarnation observations.
    History,
    /// Admission capacity.
    Capacity,
}

impl ApplicationErrorCategory {
    /// Returns the stable machine name.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Input => "input",
            Self::Authorization => "authorization",
            Self::Contract => "contract",
            Self::Query => "query",
            Self::Module => "module",
            Self::Cursor => "cursor",
            Self::Resource => "resource",
            Self::Storage => "storage",
            Self::Uncertainty => "uncertainty",
            Self::Internal => "internal",
            Self::Protocol => "protocol",
            Self::Command => "command",
            Self::Control => "control",
            Self::History => "history",
            Self::Capacity => "capacity",
        }
    }
}

/// Closed application recovery action.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum ApplicationRecoveryAction {
    /// Correct the input before retrying.
    CorrectRequest,
    /// Retry according to caller policy.
    Retry,
    /// Resolve using the same idempotency identity.
    ResolveWithSameIdempotencyKey,
    /// Bind or obtain an application role.
    ObtainPermission,
    /// Refresh contract and module metadata.
    RefreshContract,
    /// Escalate with the incident identifier when present.
    ContactOperator,
    /// No recovery is prescribed.
    None,
}

impl ApplicationRecoveryAction {
    /// Returns the stable machine name.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::CorrectRequest => "correct_request",
            Self::Retry => "retry",
            Self::ResolveWithSameIdempotencyKey => "resolve_with_same_idempotency_key",
            Self::ObtainPermission => "obtain_permission",
            Self::RefreshContract => "refresh_contract",
            Self::ContactOperator => "contact_operator",
            Self::None => "none",
        }
    }
}

/// Closed machine-actionable remediation.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum ApplicationFixCode {
    /// Correct the typed input or query source.
    CorrectInput,
    /// Remove a result field not visible to the application role.
    RemoveForbiddenOutput,
    /// Bind the symbolic application role.
    BindApplicationRole,
    /// Refresh exact contract metadata.
    RefreshContract,
    /// Pin or redeploy the active named-query module.
    PinActiveModule,
    /// Declare a bounded index that satisfies the diagnostic.
    AddBoundedIndex,
    /// Discard a stale cursor and restart.
    RestartFromFirstPage,
    /// Retry later using bounded caller policy.
    RetryLater,
    /// Resolve or retry with the exact same idempotency identity.
    ResolveWithSameIdempotencyKey,
    /// Contact an operator and include only the opaque incident ID.
    ContactOperatorWithIncident,
}

impl ApplicationFixCode {
    /// Returns the stable machine name.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::CorrectInput => "correct_input",
            Self::RemoveForbiddenOutput => "remove_forbidden_output",
            Self::BindApplicationRole => "bind_application_role",
            Self::RefreshContract => "refresh_contract",
            Self::PinActiveModule => "pin_active_module",
            Self::AddBoundedIndex => "add_bounded_index",
            Self::RestartFromFirstPage => "restart_from_first_page",
            Self::RetryLater => "retry_later",
            Self::ResolveWithSameIdempotencyKey => "resolve_with_same_idempotency_key",
            Self::ContactOperatorWithIncident => "contact_operator_with_incident",
        }
    }
}

/// Checked half-open source span in submitted RiffQL.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct ApplicationSourceSpan {
    start: u64,
    end: u64,
}

impl ApplicationSourceSpan {
    /// Constructs a bounded ordered source span.
    pub const fn new(start: u64, end: u64) -> Option<Self> {
        if start <= end && end <= MAX_APPLICATION_SOURCE_OFFSET {
            Some(Self { start, end })
        } else {
            None
        }
    }

    /// Inclusive start byte offset.
    #[must_use]
    pub const fn start(self) -> u64 {
        self.start
    }

    /// Exclusive end byte offset.
    #[must_use]
    pub const fn end(self) -> u64 {
        self.end
    }
}

/// Bounded symbolic context for one application failure.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ApplicationErrorContext {
    contract: Option<(ContractLineage, ContractVersion)>,
    operation_symbol: Option<String>,
    symbol_path: Vec<String>,
    source_span: Option<ApplicationSourceSpan>,
    trace_id: Option<RequestId>,
}

impl ApplicationErrorContext {
    /// Constructs empty context for a failure that has no honest symbol.
    #[must_use]
    pub const fn empty() -> Self {
        Self {
            contract: None,
            operation_symbol: None,
            symbol_path: Vec::new(),
            source_span: None,
            trace_id: None,
        }
    }

    /// Adds exact contract identity already visible to the caller.
    #[must_use]
    pub fn with_contract(mut self, lineage: ContractLineage, version: ContractVersion) -> Self {
        self.contract = Some((lineage, version));
        self
    }

    /// Adds one checked caller-visible query, module, command, or role symbol.
    pub fn with_operation_symbol(
        mut self,
        symbol: String,
    ) -> Result<Self, ApplicationErrorContextError> {
        validate_application_symbol(&symbol)?;
        self.operation_symbol = Some(symbol);
        Ok(self)
    }

    /// Adds a bounded checked caller-visible symbol path.
    pub fn with_symbol_path(
        mut self,
        symbols: Vec<String>,
    ) -> Result<Self, ApplicationErrorContextError> {
        if symbols.len() > MAX_APPLICATION_SYMBOL_PATH_SEGMENTS {
            return Err(ApplicationErrorContextError);
        }
        for symbol in &symbols {
            validate_application_symbol(symbol)?;
        }
        self.symbol_path = symbols;
        Ok(self)
    }

    /// Adds a checked source span.
    #[must_use]
    pub const fn with_source_span(mut self, source_span: ApplicationSourceSpan) -> Self {
        self.source_span = Some(source_span);
        self
    }

    /// Adds the request identity as the safe end-to-end trace identity.
    #[must_use]
    pub const fn with_trace_id(mut self, trace_id: RequestId) -> Self {
        self.trace_id = Some(trace_id);
        self
    }

    /// Exact selected contract, when safely known.
    #[must_use]
    pub const fn contract(&self) -> Option<&(ContractLineage, ContractVersion)> {
        self.contract.as_ref()
    }

    /// Caller-visible operation symbol, when safely known.
    #[must_use]
    pub fn operation_symbol(&self) -> Option<&str> {
        self.operation_symbol.as_deref()
    }

    /// Caller-visible symbolic path.
    #[must_use]
    pub fn symbol_path(&self) -> &[String] {
        &self.symbol_path
    }

    /// Source span, when safely known.
    #[must_use]
    pub const fn source_span(&self) -> Option<ApplicationSourceSpan> {
        self.source_span
    }

    /// Opaque request/trace identity.
    #[must_use]
    pub const fn trace_id(&self) -> Option<RequestId> {
        self.trace_id
    }
}

fn validate_application_symbol(symbol: &str) -> Result<(), ApplicationErrorContextError> {
    if symbol.is_empty()
        || symbol.len() > MAX_APPLICATION_SYMBOL_BYTES
        || !symbol
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.'))
    {
        return Err(ApplicationErrorContextError);
    }
    Ok(())
}

/// A symbolic context rejected before it could reach a public error.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ApplicationErrorContextError;

impl fmt::Display for ApplicationErrorContextError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("application error context is invalid")
    }
}

impl Error for ApplicationErrorContextError {}

/// One fully checked bounded application-semantic failure.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ApplicationError {
    code: ApplicationErrorCode,
    operation: ApplicationOperation,
    context: ApplicationErrorContext,
    incident_id: Option<IncidentId>,
}

impl ApplicationError {
    /// Constructs one registry-owned error with already checked context.
    #[must_use]
    pub const fn new(
        code: ApplicationErrorCode,
        operation: ApplicationOperation,
        context: ApplicationErrorContext,
        incident_id: Option<IncidentId>,
    ) -> Self {
        Self {
            code,
            operation,
            context,
            incident_id,
        }
    }

    /// Lifts one compatible kernel failure into symbolic application context.
    ///
    /// Kernel validation paths are intentionally not copied because they carry
    /// compiler IDs rather than application symbols.
    #[must_use]
    pub fn from_public_error(
        error: &PublicError,
        operation: ApplicationOperation,
        context: ApplicationErrorContext,
    ) -> Self {
        Self::new(
            error
                .application_code_hint()
                .unwrap_or_else(|| ApplicationErrorCode::from_public_kind(error.kind())),
            operation,
            context,
            error.incident_id().copied(),
        )
    }

    /// Stable code.
    #[must_use]
    pub const fn code(&self) -> ApplicationErrorCode {
        self.code
    }

    /// Application operation.
    #[must_use]
    pub const fn operation(&self) -> ApplicationOperation {
        self.operation
    }

    /// Checked safe context.
    #[must_use]
    pub const fn context(&self) -> &ApplicationErrorContext {
        &self.context
    }

    /// Opaque incident identity.
    #[must_use]
    pub const fn incident_id(&self) -> Option<&IncidentId> {
        self.incident_id.as_ref()
    }

    /// Closed category.
    #[must_use]
    pub const fn category(&self) -> ApplicationErrorCategory {
        self.code.category()
    }

    /// Stable public message.
    #[must_use]
    pub const fn safe_message(&self) -> &'static str {
        self.code.safe_message()
    }

    /// Recovery action.
    #[must_use]
    pub const fn recovery_action(&self) -> ApplicationRecoveryAction {
        self.code.recovery_action()
    }

    /// Deterministic remediation codes.
    #[must_use]
    pub const fn fixes(&self) -> &'static [ApplicationFixCode] {
        self.code.fixes()
    }
}

impl fmt::Display for ApplicationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "{}: {} [{}]",
            self.code.as_str(),
            self.safe_message(),
            self.operation.as_str()
        )?;
        if let Some(symbol) = self.context.operation_symbol() {
            write!(formatter, " {symbol}")?;
        }
        if let Some(incident_id) = &self.incident_id {
            write!(formatter, " (incident {incident_id})")?;
        }
        Ok(())
    }
}

impl Error for ApplicationError {}

/// Consumer-owned source for fresh opaque incident identifiers.
///
/// Production providers live at the server composition boundary. Core error
/// handling receives only this synchronous port and must fail closed when it
/// cannot obtain an identifier.
pub trait IncidentIdSource: Send + Sync {
    /// Returns one fresh checked UUIDv7 incident identifier.
    fn next_incident_id(&self) -> Result<IncidentId, IncidentIdSourceError>;
}

/// A bounded, public-safe failure to obtain a fresh incident identifier.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct IncidentIdSourceError;

impl fmt::Display for IncidentIdSourceError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("incident identifier source failed")
    }
}

impl Error for IncidentIdSourceError {}

/// The maximum number of validation issues returned for one request.
pub const MAX_VALIDATION_ISSUES: usize = 16;

/// The maximum number of segments in one validation path.
pub const MAX_VALIDATION_PATH_SEGMENTS: usize = 16;

/// A protocol-neutral classification for a public failure.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum ErrorClass {
    /// The submitted request is invalid and must be corrected.
    InvalidArgument,
    /// The request conflicts with previously admitted state.
    Conflict,
    /// The principal is not permitted to perform the operation.
    PermissionDenied,
    /// The operation did not complete before its deadline.
    DeadlineExceeded,
    /// The request targets a contract state that is no longer current.
    FailedPrecondition,
    /// A required service is temporarily unavailable.
    Unavailable,
    /// The caller must resolve an uncertain command result.
    Uncertain,
    /// The server encountered a defect that is not safe to disclose.
    Internal,
}

/// Safe, protocol-neutral recovery guidance for a public failure.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum RecoveryAction {
    /// Correct the request before submitting it again.
    CorrectRequest,
    /// Retry the operation according to the caller's retry policy.
    Retry,
    /// Resolve or retry the command with the original idempotency key.
    ResolveWithSameIdempotencyKey,
    /// Obtain an appropriate permission before retrying.
    ObtainPermission,
    /// Refresh contract metadata and rebuild the request.
    RefreshContract,
    /// Escalate the incident to an operator.
    ContactOperator,
}

/// Closed emergency failure when an internal incident cannot be identified.
///
/// This value is intentionally distinct from [`PublicError`]: an internal
/// public error requires a real fresh [`IncidentId`]. It carries no identifier,
/// arbitrary text, diagnostic source, or protected operation result.
///
/// ```compile_fail
/// use riffdb_errors::{EmergencyInternalFailure, IncidentIdSourceError, PublicError};
///
/// let emergency = EmergencyInternalFailure::from(IncidentIdSourceError);
/// let _: PublicError = emergency.into();
/// ```
#[derive(Clone, Copy, Eq, PartialEq)]
pub struct EmergencyInternalFailure {
    _private: (),
}

impl EmergencyInternalFailure {
    /// Returns the only fixed message safe at an emergency transport boundary.
    #[must_use]
    pub const fn safe_message(self) -> &'static str {
        "an internal error occurred"
    }

    /// Returns the protocol-neutral emergency classification.
    #[must_use]
    pub const fn class(self) -> ErrorClass {
        ErrorClass::Internal
    }

    /// Returns the only safe recovery guidance for this readiness failure.
    #[must_use]
    pub const fn recovery_action(self) -> RecoveryAction {
        RecoveryAction::ContactOperator
    }
}

impl From<IncidentIdSourceError> for EmergencyInternalFailure {
    fn from(_: IncidentIdSourceError) -> Self {
        Self { _private: () }
    }
}

impl fmt::Display for EmergencyInternalFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.safe_message())
    }
}

impl fmt::Debug for EmergencyInternalFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("EmergencyInternalFailure")
    }
}

impl Error for EmergencyInternalFailure {}

/// The closed set of failures that RiffDB may expose to an untrusted caller.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum PublicErrorKind {
    /// A request failed bounded validation.
    Validation,
    /// An idempotency identity was reused with different canonical input.
    IdempotencyKeyReuse,
    /// Authorization denied the operation.
    AuthorizationDenied,
    /// Logical conflict acquisition exceeded its deadline, or the fixed v1
    /// command reevaluation budget was exhausted.
    ConcurrencyDeadlineExceeded,
    /// The requested contract version or plan is not current.
    ContractMismatch,
    /// Authoritative storage is temporarily unavailable.
    StorageUnavailable,
    /// A submitted command may have completed, but its outcome is not known.
    OutcomeUnknown,
    /// An internal defect occurred and details were retained only internally.
    InternalDefect,
    /// Deterministic evaluation failed before an application commit was formed.
    CommandExecutionFailed,
    /// Observed history predates a database restore.
    HistoryIncarnationMismatch,
    /// Requested history was retired by retention prune.
    HistoryPruned,
    /// The service is over capacity and rejected admission.
    Overloaded,
}

impl PublicErrorKind {
    /// Returns the stable ASCII identifier exposed by every transport.
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::Validation => "validation_failed",
            Self::IdempotencyKeyReuse => "idempotency_key_reuse",
            Self::AuthorizationDenied => "authorization_denied",
            Self::ConcurrencyDeadlineExceeded => "concurrency_deadline_exceeded",
            Self::ContractMismatch => "contract_mismatch",
            Self::StorageUnavailable => "storage_unavailable",
            Self::OutcomeUnknown => "outcome_unknown",
            Self::InternalDefect => "internal_defect",
            Self::CommandExecutionFailed => "command_execution_failed",
            Self::HistoryIncarnationMismatch => "history_incarnation_mismatch",
            Self::HistoryPruned => "history_pruned",
            Self::Overloaded => "overloaded",
        }
    }

    /// Returns the stable, caller-safe ASCII message for this failure.
    #[must_use]
    pub const fn safe_message(self) -> &'static str {
        match self {
            Self::Validation => "request validation failed",
            Self::IdempotencyKeyReuse => "idempotency key was reused with different input",
            Self::AuthorizationDenied => "operation is not authorized",
            Self::ConcurrencyDeadlineExceeded => "concurrency deadline exceeded",
            Self::ContractMismatch => "contract version or plan does not match",
            Self::StorageUnavailable => "storage is temporarily unavailable",
            Self::OutcomeUnknown => "command outcome is not yet known",
            Self::InternalDefect => "an internal error occurred",
            Self::CommandExecutionFailed => "command execution failed",
            Self::HistoryIncarnationMismatch => "observed history predates a database restore",
            Self::HistoryPruned => "requested history has been pruned",
            Self::Overloaded => "service is over capacity",
        }
    }

    /// Returns the protocol-neutral class for this failure.
    #[must_use]
    pub const fn class(self) -> ErrorClass {
        match self {
            Self::Validation => ErrorClass::InvalidArgument,
            Self::IdempotencyKeyReuse => ErrorClass::Conflict,
            Self::AuthorizationDenied => ErrorClass::PermissionDenied,
            Self::ConcurrencyDeadlineExceeded => ErrorClass::DeadlineExceeded,
            Self::ContractMismatch | Self::HistoryIncarnationMismatch | Self::HistoryPruned => {
                ErrorClass::FailedPrecondition
            }
            Self::StorageUnavailable | Self::Overloaded => ErrorClass::Unavailable,
            Self::OutcomeUnknown => ErrorClass::Uncertain,
            Self::InternalDefect => ErrorClass::Internal,
            Self::CommandExecutionFailed => ErrorClass::FailedPrecondition,
        }
    }

    /// Returns the one authoritative public status code for this kind.
    ///
    /// Server encoding and client validation must both use this mapping. Do not
    /// re-derive the wire code from [`Self::class`] alone: some kinds deliberately
    /// diverge (for example, overload keeps class [`ErrorClass::Unavailable`] while
    /// carrying [`PublicErrorStatusCode::ResourceExhausted`]).
    ///
    /// This match is the sole wire-code authority and must remain exhaustive over
    /// every [`PublicErrorKind`] variant.
    #[must_use]
    pub const fn status_code(self) -> PublicErrorStatusCode {
        match self {
            Self::Validation => PublicErrorStatusCode::InvalidArgument,
            Self::IdempotencyKeyReuse => PublicErrorStatusCode::AlreadyExists,
            Self::AuthorizationDenied => PublicErrorStatusCode::PermissionDenied,
            Self::ConcurrencyDeadlineExceeded => PublicErrorStatusCode::DeadlineExceeded,
            Self::ContractMismatch
            | Self::CommandExecutionFailed
            | Self::HistoryIncarnationMismatch
            | Self::HistoryPruned => PublicErrorStatusCode::FailedPrecondition,
            Self::StorageUnavailable => PublicErrorStatusCode::Unavailable,
            Self::Overloaded => PublicErrorStatusCode::ResourceExhausted,
            Self::OutcomeUnknown => PublicErrorStatusCode::Unknown,
            Self::InternalDefect => PublicErrorStatusCode::Internal,
        }
    }

    /// Returns safe recovery guidance for this failure.
    #[must_use]
    pub const fn recovery_action(self) -> RecoveryAction {
        match self {
            Self::Validation
            | Self::IdempotencyKeyReuse
            | Self::HistoryIncarnationMismatch
            | Self::HistoryPruned => RecoveryAction::CorrectRequest,
            Self::AuthorizationDenied => RecoveryAction::ObtainPermission,
            Self::ConcurrencyDeadlineExceeded | Self::StorageUnavailable | Self::Overloaded => {
                RecoveryAction::Retry
            }
            Self::ContractMismatch => RecoveryAction::RefreshContract,
            Self::OutcomeUnknown => RecoveryAction::ResolveWithSameIdempotencyKey,
            Self::InternalDefect | Self::CommandExecutionFailed => RecoveryAction::ContactOperator,
        }
    }
}

/// Authoritative status code carried for a public error on the gRPC wire.
///
/// Transport adapters map this closed set to their native status type. It is
/// intentionally separate from [`ErrorClass`]: class is the protocol-neutral
/// classification, while this value is the exact wire status both server and
/// client must agree on.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum PublicErrorStatusCode {
    /// The submitted request is invalid and must be corrected.
    InvalidArgument,
    /// The request conflicts with previously admitted state.
    AlreadyExists,
    /// The principal is not permitted to perform the operation.
    PermissionDenied,
    /// The operation did not complete before its deadline.
    DeadlineExceeded,
    /// The request targets state that is no longer current.
    FailedPrecondition,
    /// Capacity or resource bounds rejected the request without execution.
    ResourceExhausted,
    /// A required service is temporarily unavailable.
    Unavailable,
    /// The caller must resolve an uncertain command result.
    Unknown,
    /// The server encountered a defect that is not safe to disclose.
    Internal,
}

/// A stable, caller-safe explanation for one validation failure.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum ValidationCode {
    /// A required value was not provided.
    MissingRequiredValue,
    /// A value has the wrong contract type.
    TypeMismatch,
    /// A value has an invalid representation.
    InvalidValue,
    /// A numeric or temporal value is outside its allowed range.
    OutOfRange,
    /// A string or byte value exceeds its allowed length.
    TooLong,
    /// A collection exceeds its allowed item count.
    TooManyItems,
    /// A submitted field is not part of the active contract.
    UnknownField,
    /// A submitted record contains a field more than once.
    DuplicateField,
}

impl ValidationCode {
    /// Returns the stable ASCII identifier exposed by every transport.
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::MissingRequiredValue => "missing_required_value",
            Self::TypeMismatch => "type_mismatch",
            Self::InvalidValue => "invalid_value",
            Self::OutOfRange => "out_of_range",
            Self::TooLong => "too_long",
            Self::TooManyItems => "too_many_items",
            Self::UnknownField => "unknown_field",
            Self::DuplicateField => "duplicate_field",
        }
    }

    /// Returns a stable public-safe description of the validation failure.
    #[must_use]
    pub const fn safe_message(self) -> &'static str {
        match self {
            Self::MissingRequiredValue => "required value is missing",
            Self::TypeMismatch => "value has the wrong type",
            Self::InvalidValue => "value is invalid",
            Self::OutOfRange => "value is outside the allowed range",
            Self::TooLong => "value exceeds the allowed length",
            Self::TooManyItems => "collection has too many items",
            Self::UnknownField => "field is not defined by the active contract",
            Self::DuplicateField => "field occurs more than once",
        }
    }
}

/// One non-secret location component in a validation issue.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum ValidationPathSegment {
    /// A compiler-assigned field identifier.
    Field(FieldId),
    /// A zero-based list position.
    ListIndex(u32),
}

/// A bounded path to a value that failed validation.
#[derive(Clone, Debug, Default, Eq, Hash, PartialEq)]
pub struct ValidationPath {
    segments: Vec<ValidationPathSegment>,
}

impl ValidationPath {
    /// Creates the root path, used when an issue applies to the whole request.
    #[must_use]
    pub const fn root() -> Self {
        Self {
            segments: Vec::new(),
        }
    }

    /// Creates a checked validation path.
    pub fn new(segments: Vec<ValidationPathSegment>) -> Result<Self, ValidationPathError> {
        if segments.len() > MAX_VALIDATION_PATH_SEGMENTS {
            return Err(ValidationPathError::TooManySegments);
        }
        Ok(Self { segments })
    }

    /// Returns the path components in root-to-leaf order.
    #[must_use]
    pub fn segments(&self) -> &[ValidationPathSegment] {
        &self.segments
    }
}

/// A failure to construct a bounded validation path.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ValidationPathError {
    /// More than [`MAX_VALIDATION_PATH_SEGMENTS`] were supplied.
    TooManySegments,
}

impl fmt::Display for ValidationPathError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("validation path has too many segments")
    }
}

impl Error for ValidationPathError {}

/// One stable validation code associated with a structured path.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct ValidationIssue {
    code: ValidationCode,
    path: ValidationPath,
}

impl ValidationIssue {
    /// Creates a validation issue without accepting caller-controlled text.
    #[must_use]
    pub const fn new(code: ValidationCode, path: ValidationPath) -> Self {
        Self { code, path }
    }

    /// Returns the stable validation code.
    #[must_use]
    pub const fn code(&self) -> ValidationCode {
        self.code
    }

    /// Returns the structured location of the invalid value.
    #[must_use]
    pub const fn path(&self) -> &ValidationPath {
        &self.path
    }
}

/// A non-empty bounded collection of validation issues.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct ValidationIssues {
    issues: Vec<ValidationIssue>,
}

impl ValidationIssues {
    /// Creates a checked collection of validation issues.
    pub fn new(issues: Vec<ValidationIssue>) -> Result<Self, ValidationIssuesError> {
        if issues.is_empty() {
            return Err(ValidationIssuesError::Empty);
        }
        if issues.len() > MAX_VALIDATION_ISSUES {
            return Err(ValidationIssuesError::TooManyIssues);
        }
        Ok(Self { issues })
    }

    /// Creates a collection containing exactly one issue.
    #[must_use]
    pub fn one(issue: ValidationIssue) -> Self {
        Self {
            issues: vec![issue],
        }
    }

    /// Returns the validation issues in deterministic producer order.
    #[must_use]
    pub fn as_slice(&self) -> &[ValidationIssue] {
        &self.issues
    }
}

/// A failure to construct a bounded collection of validation issues.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ValidationIssuesError {
    /// At least one validation issue is required.
    Empty,
    /// More than [`MAX_VALIDATION_ISSUES`] were supplied.
    TooManyIssues,
}

impl fmt::Display for ValidationIssuesError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Empty => "validation issue collection is empty",
            Self::TooManyIssues => "validation issue collection has too many entries",
        })
    }
}

impl Error for ValidationIssuesError {}

/// Structured, caller-safe detail for a public failure.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PublicErrorDetails {
    /// The failure does not expose structured detail.
    None,
    /// Bounded validation failures.
    Validation(ValidationIssues),
    /// The authoritative contract version active at the time of rejection.
    ContractMismatch {
        /// The contract version the caller must refresh to.
        active_contract_version: ContractVersion,
    },
    /// A closed deterministic evaluation failure code.
    CommandExecutionFailed {
        /// The arithmetic or fixed resource-limit failure classification.
        code: ExecutionFailureCode,
    },
}

/// A failure safe to display, serialize, or map at a public transport boundary.
///
/// This type cannot carry arbitrary messages or an error source. Construction
/// is restricted to named constructors so its kind and details cannot disagree.
/// The optional incident identifier is an opaque correlation value, not a
/// diagnostic.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PublicError {
    kind: PublicErrorKind,
    details: PublicErrorDetails,
    incident_id: Option<IncidentId>,
    application_code_hint: Option<ApplicationErrorCode>,
}

impl PublicError {
    const fn contextless(kind: PublicErrorKind) -> Self {
        Self {
            kind,
            details: PublicErrorDetails::None,
            incident_id: None,
            application_code_hint: None,
        }
    }

    /// Creates a bounded validation failure.
    #[must_use]
    pub const fn validation(issues: ValidationIssues) -> Self {
        Self {
            kind: PublicErrorKind::Validation,
            details: PublicErrorDetails::Validation(issues),
            incident_id: None,
            application_code_hint: None,
        }
    }

    /// Creates an idempotency-key-reuse failure.
    #[must_use]
    pub const fn idempotency_key_reuse() -> Self {
        Self::contextless(PublicErrorKind::IdempotencyKeyReuse)
    }

    /// Creates an authorization-denied failure.
    #[must_use]
    pub const fn authorization_denied() -> Self {
        Self::contextless(PublicErrorKind::AuthorizationDenied)
    }

    /// Creates the shared failure for a logical-conflict deadline or exhausted
    /// fixed v1 command reevaluation budget.
    #[must_use]
    pub const fn concurrency_deadline_exceeded() -> Self {
        Self::contextless(PublicErrorKind::ConcurrencyDeadlineExceeded)
    }

    /// Creates a contract mismatch and identifies the active version.
    #[must_use]
    pub const fn contract_mismatch(active_contract_version: ContractVersion) -> Self {
        Self {
            kind: PublicErrorKind::ContractMismatch,
            details: PublicErrorDetails::ContractMismatch {
                active_contract_version,
            },
            incident_id: None,
            application_code_hint: None,
        }
    }

    /// Creates a storage-unavailable failure.
    #[must_use]
    pub const fn storage_unavailable() -> Self {
        Self::contextless(PublicErrorKind::StorageUnavailable)
    }

    /// Creates an uncertain-outcome failure.
    #[must_use]
    pub const fn outcome_unknown() -> Self {
        Self::contextless(PublicErrorKind::OutcomeUnknown)
    }

    /// Creates a deterministic command-execution failure.
    #[must_use]
    pub const fn command_execution_failed(code: ExecutionFailureCode) -> Self {
        Self {
            kind: PublicErrorKind::CommandExecutionFailed,
            details: PublicErrorDetails::CommandExecutionFailed { code },
            incident_id: None,
            application_code_hint: None,
        }
    }

    /// Creates a redacted internal-defect failure with its incident identifier.
    #[must_use]
    pub const fn internal_defect(incident_id: IncidentId) -> Self {
        Self {
            kind: PublicErrorKind::InternalDefect,
            details: PublicErrorDetails::None,
            incident_id: Some(incident_id),
            application_code_hint: None,
        }
    }

    /// Creates a history-incarnation mismatch failure.
    #[must_use]
    pub const fn history_incarnation_mismatch() -> Self {
        Self::contextless(PublicErrorKind::HistoryIncarnationMismatch)
    }

    /// Creates a history-pruned failure (RDB-HISTORY-0102).
    #[must_use]
    pub const fn history_pruned() -> Self {
        Self::contextless(PublicErrorKind::HistoryPruned)
    }

    /// Creates an overload / capacity rejection failure.
    #[must_use]
    pub const fn overloaded() -> Self {
        Self::contextless(PublicErrorKind::Overloaded)
    }

    /// Adds an opaque incident identifier for trusted diagnostic correlation.
    #[must_use]
    pub const fn with_incident_id(mut self, incident_id: IncidentId) -> Self {
        self.incident_id = Some(incident_id);
        self
    }

    /// Attaches a stricter symbolic classification for the application surface.
    ///
    /// The compatible kernel serializer deliberately ignores this hint.
    pub fn with_application_code_hint(
        mut self,
        code: ApplicationErrorCode,
    ) -> Result<Self, ApplicationErrorHintError> {
        let compatible = match self.kind {
            PublicErrorKind::Validation => matches!(
                code,
                ApplicationErrorCode::InvalidRequest
                    | ApplicationErrorCode::InputInvalid
                    | ApplicationErrorCode::QueryInvalid
                    | ApplicationErrorCode::QueryUnavailable
                    | ApplicationErrorCode::ModuleUnavailable
                    | ApplicationErrorCode::CursorInvalid
            ),
            PublicErrorKind::AuthorizationDenied => matches!(
                code,
                ApplicationErrorCode::AuthorizationDenied | ApplicationErrorCode::CapabilityRevoked
            ),
            PublicErrorKind::ContractMismatch => code == ApplicationErrorCode::ContractMismatch,
            PublicErrorKind::StorageUnavailable => code == ApplicationErrorCode::StorageUnavailable,
            PublicErrorKind::OutcomeUnknown => code == ApplicationErrorCode::OutcomeUnknown,
            PublicErrorKind::InternalDefect => code == ApplicationErrorCode::InternalDefect,
            PublicErrorKind::IdempotencyKeyReuse => {
                code == ApplicationErrorCode::IdempotencyKeyReuse
            }
            PublicErrorKind::ConcurrencyDeadlineExceeded => {
                code == ApplicationErrorCode::DeadlineExceeded
            }
            PublicErrorKind::CommandExecutionFailed => {
                code == ApplicationErrorCode::CommandExecutionFailed
            }
            PublicErrorKind::HistoryIncarnationMismatch => {
                code == ApplicationErrorCode::HistoryIncarnationMismatch
            }
            PublicErrorKind::HistoryPruned => code == ApplicationErrorCode::HistoryPruned,
            PublicErrorKind::Overloaded => code == ApplicationErrorCode::Overloaded,
        };
        if !compatible {
            return Err(ApplicationErrorHintError);
        }
        self.application_code_hint = Some(code);
        Ok(self)
    }

    /// Returns the stricter application-only classification, when present.
    #[must_use]
    pub const fn application_code_hint(&self) -> Option<ApplicationErrorCode> {
        self.application_code_hint
    }

    /// Returns the closed failure kind.
    #[must_use]
    pub const fn kind(&self) -> PublicErrorKind {
        self.kind
    }

    /// Returns the stable public code.
    #[must_use]
    pub const fn code(&self) -> &'static str {
        self.kind.code()
    }

    /// Returns the stable public-safe message.
    #[must_use]
    pub const fn safe_message(&self) -> &'static str {
        self.kind.safe_message()
    }

    /// Returns the protocol-neutral failure class.
    #[must_use]
    pub const fn class(&self) -> ErrorClass {
        self.kind.class()
    }

    /// Returns the authoritative public status code for this error.
    #[must_use]
    pub const fn status_code(&self) -> PublicErrorStatusCode {
        self.kind.status_code()
    }

    /// Returns safe recovery guidance.
    #[must_use]
    pub const fn recovery_action(&self) -> RecoveryAction {
        self.kind.recovery_action()
    }

    /// Returns structured, caller-safe detail for this failure.
    #[must_use]
    pub const fn details(&self) -> &PublicErrorDetails {
        &self.details
    }

    /// Returns the opaque incident identifier, when one is available.
    #[must_use]
    pub const fn incident_id(&self) -> Option<&IncidentId> {
        self.incident_id.as_ref()
    }
}

/// An application hint did not refine the compatible kernel classification.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ApplicationErrorHintError;

impl fmt::Display for ApplicationErrorHintError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("application error hint is incompatible")
    }
}

impl Error for ApplicationErrorHintError {}

impl fmt::Display for PublicError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}: {}", self.code(), self.safe_message())?;
        if let Some(incident_id) = &self.incident_id {
            write!(formatter, " (incident {incident_id})")?;
        }
        Ok(())
    }
}

impl Error for PublicError {}

/// An internal failure retaining its source for trusted tracing.
///
/// Its default [`fmt::Display`] and [`fmt::Debug`] representations are
/// redacted. Converting this type into [`PublicError`] deliberately discards
/// the source.
pub struct InternalError {
    incident_id: IncidentId,
    source: Box<dyn Error + Send + Sync + 'static>,
}

impl InternalError {
    /// Creates an internal failure associated with an opaque incident.
    pub fn new(incident_id: IncidentId, source: impl Error + Send + Sync + 'static) -> Self {
        Self {
            incident_id,
            source: Box::new(source),
        }
    }

    /// Returns the opaque identifier used to correlate trusted diagnostics.
    #[must_use]
    pub const fn incident_id(&self) -> &IncidentId {
        &self.incident_id
    }

    /// Converts this internal failure into a redacted public failure.
    #[must_use]
    pub fn into_public(self) -> PublicError {
        PublicError::internal_defect(self.incident_id)
    }
}

impl fmt::Display for InternalError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "internal defect (incident {})", self.incident_id)
    }
}

impl fmt::Debug for InternalError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("InternalError")
            .field("incident_id", &self.incident_id)
            .field("source", &"[REDACTED]")
            .finish()
    }
}

impl Error for InternalError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(self.source.as_ref())
    }
}

impl From<InternalError> for PublicError {
    fn from(error: InternalError) -> Self {
        error.into_public()
    }
}

#[cfg(test)]
mod tests {
    use std::fmt::Write as _;

    use super::*;

    const KINDS: [PublicErrorKind; 12] = [
        PublicErrorKind::Validation,
        PublicErrorKind::IdempotencyKeyReuse,
        PublicErrorKind::AuthorizationDenied,
        PublicErrorKind::ConcurrencyDeadlineExceeded,
        PublicErrorKind::ContractMismatch,
        PublicErrorKind::StorageUnavailable,
        PublicErrorKind::OutcomeUnknown,
        PublicErrorKind::InternalDefect,
        PublicErrorKind::CommandExecutionFailed,
        PublicErrorKind::HistoryIncarnationMismatch,
        PublicErrorKind::HistoryPruned,
        PublicErrorKind::Overloaded,
    ];

    const VALIDATION_CODES: [ValidationCode; 8] = [
        ValidationCode::MissingRequiredValue,
        ValidationCode::TypeMismatch,
        ValidationCode::InvalidValue,
        ValidationCode::OutOfRange,
        ValidationCode::TooLong,
        ValidationCode::TooManyItems,
        ValidationCode::UnknownField,
        ValidationCode::DuplicateField,
    ];

    #[derive(Debug)]
    struct SecretSource;

    impl fmt::Display for SecretSource {
        fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("secret-canary-internal-context")
        }
    }

    impl Error for SecretSource {}

    #[test]
    fn application_error_registry_matches_the_checked_fixture() {
        let mut rendered = "code\tcategory\trecovery_action\tfixes\tmessage\n".to_owned();
        for code in APPLICATION_ERROR_CODES {
            let fixes = code
                .fixes()
                .iter()
                .map(|fix| fix.as_str())
                .collect::<Vec<_>>()
                .join(",");
            writeln!(
                rendered,
                "{}\t{}\t{}\t{}\t{}",
                code.as_str(),
                code.category().as_str(),
                code.recovery_action().as_str(),
                fixes,
                code.safe_message()
            )
            .expect("string");
        }
        assert_eq!(
            rendered,
            include_str!("../../../fixtures/application-errors/registry-v1.tsv")
        );
    }

    #[test]
    fn application_hints_only_refine_compatible_kernel_failures() {
        let validation = PublicError::validation(ValidationIssues::one(ValidationIssue::new(
            ValidationCode::InvalidValue,
            ValidationPath::root(),
        )));
        let cursor = validation
            .clone()
            .with_application_code_hint(ApplicationErrorCode::CursorInvalid)
            .expect("compatible cursor refinement");
        assert_eq!(
            cursor.application_code_hint(),
            Some(ApplicationErrorCode::CursorInvalid)
        );
        assert_eq!(
            validation.with_application_code_hint(ApplicationErrorCode::StorageUnavailable),
            Err(ApplicationErrorHintError)
        );
        assert_eq!(
            PublicError::authorization_denied()
                .with_application_code_hint(ApplicationErrorCode::QueryInvalid),
            Err(ApplicationErrorHintError)
        );
    }

    fn incident_id() -> IncidentId {
        let mut bytes = [0x42; 16];
        bytes[6] = 0x72;
        bytes[8] = 0x82;
        IncidentId::from_bytes(bytes).expect("fixture is a valid UUIDv7")
    }

    fn issue() -> ValidationIssue {
        ValidationIssue::new(
            ValidationCode::TypeMismatch,
            ValidationPath::new(vec![
                ValidationPathSegment::Field(FieldId::new(7).expect("field ID is nonzero")),
                ValidationPathSegment::ListIndex(3),
            ])
            .expect("fixture path is bounded"),
        )
    }

    #[test]
    fn codes_messages_classes_and_recovery_actions_are_stable() {
        let expected = [
            (
                "validation_failed",
                "request validation failed",
                ErrorClass::InvalidArgument,
                RecoveryAction::CorrectRequest,
            ),
            (
                "idempotency_key_reuse",
                "idempotency key was reused with different input",
                ErrorClass::Conflict,
                RecoveryAction::CorrectRequest,
            ),
            (
                "authorization_denied",
                "operation is not authorized",
                ErrorClass::PermissionDenied,
                RecoveryAction::ObtainPermission,
            ),
            (
                "concurrency_deadline_exceeded",
                "concurrency deadline exceeded",
                ErrorClass::DeadlineExceeded,
                RecoveryAction::Retry,
            ),
            (
                "contract_mismatch",
                "contract version or plan does not match",
                ErrorClass::FailedPrecondition,
                RecoveryAction::RefreshContract,
            ),
            (
                "storage_unavailable",
                "storage is temporarily unavailable",
                ErrorClass::Unavailable,
                RecoveryAction::Retry,
            ),
            (
                "outcome_unknown",
                "command outcome is not yet known",
                ErrorClass::Uncertain,
                RecoveryAction::ResolveWithSameIdempotencyKey,
            ),
            (
                "internal_defect",
                "an internal error occurred",
                ErrorClass::Internal,
                RecoveryAction::ContactOperator,
            ),
            (
                "command_execution_failed",
                "command execution failed",
                ErrorClass::FailedPrecondition,
                RecoveryAction::ContactOperator,
            ),
            (
                "history_incarnation_mismatch",
                "observed history predates a database restore",
                ErrorClass::FailedPrecondition,
                RecoveryAction::CorrectRequest,
            ),
            (
                "history_pruned",
                "requested history has been pruned",
                ErrorClass::FailedPrecondition,
                RecoveryAction::CorrectRequest,
            ),
            (
                "overloaded",
                "service is over capacity",
                ErrorClass::Unavailable,
                RecoveryAction::Retry,
            ),
        ];

        assert_eq!(KINDS.len(), 12);
        assert_eq!(KINDS[9], PublicErrorKind::HistoryIncarnationMismatch);
        assert_eq!(KINDS[10], PublicErrorKind::HistoryPruned);
        assert_eq!(KINDS[11], PublicErrorKind::Overloaded);
        assert_eq!(APPLICATION_ERROR_CODES.len(), 21);
        assert_eq!(
            APPLICATION_ERROR_CODES[18],
            ApplicationErrorCode::HistoryIncarnationMismatch
        );
        assert_eq!(
            APPLICATION_ERROR_CODES[19],
            ApplicationErrorCode::HistoryPruned
        );
        assert_eq!(
            APPLICATION_ERROR_CODES[20],
            ApplicationErrorCode::Overloaded
        );

        for (kind, (code, message, class, recovery_action)) in KINDS.into_iter().zip(expected) {
            assert_eq!(kind.code(), code);
            assert_eq!(kind.safe_message(), message);
            assert_eq!(kind.class(), class);
            assert_eq!(kind.recovery_action(), recovery_action);
            assert!(code.is_ascii());
            assert!(message.is_ascii());
        }
    }

    #[test]
    fn every_kind_has_an_authoritative_wire_status_code() {
        let expected = [
            PublicErrorStatusCode::InvalidArgument,
            PublicErrorStatusCode::AlreadyExists,
            PublicErrorStatusCode::PermissionDenied,
            PublicErrorStatusCode::DeadlineExceeded,
            PublicErrorStatusCode::FailedPrecondition,
            PublicErrorStatusCode::Unavailable,
            PublicErrorStatusCode::Unknown,
            PublicErrorStatusCode::Internal,
            PublicErrorStatusCode::FailedPrecondition,
            PublicErrorStatusCode::FailedPrecondition,
            PublicErrorStatusCode::FailedPrecondition,
            PublicErrorStatusCode::ResourceExhausted,
        ];
        for (kind, expected_code) in KINDS.into_iter().zip(expected) {
            assert_eq!(kind.status_code(), expected_code);
            assert_eq!(PublicError::contextless(kind).status_code(), expected_code);
        }
        assert_eq!(PublicErrorKind::Overloaded.class(), ErrorClass::Unavailable);
        assert_eq!(
            PublicErrorKind::Overloaded.status_code(),
            PublicErrorStatusCode::ResourceExhausted
        );
    }

    #[test]
    fn staged_public_constructors_are_contextless_and_registry_aligned() {
        let mismatch = PublicError::history_incarnation_mismatch();
        assert_eq!(mismatch.kind(), PublicErrorKind::HistoryIncarnationMismatch);
        assert_eq!(mismatch.details(), &PublicErrorDetails::None);
        assert_eq!(mismatch.incident_id(), None);
        assert_eq!(
            ApplicationErrorCode::from_public_kind(mismatch.kind()),
            ApplicationErrorCode::HistoryIncarnationMismatch
        );

        let pruned = PublicError::history_pruned();
        assert_eq!(pruned.kind(), PublicErrorKind::HistoryPruned);
        assert_eq!(pruned.details(), &PublicErrorDetails::None);
        assert_eq!(pruned.incident_id(), None);
        assert_eq!(
            ApplicationErrorCode::from_public_kind(pruned.kind()),
            ApplicationErrorCode::HistoryPruned
        );

        let overloaded = PublicError::overloaded();
        assert_eq!(overloaded.kind(), PublicErrorKind::Overloaded);
        assert_eq!(overloaded.details(), &PublicErrorDetails::None);
        assert_eq!(overloaded.incident_id(), None);
        assert_eq!(
            ApplicationErrorCode::from_public_kind(overloaded.kind()),
            ApplicationErrorCode::Overloaded
        );
    }

    #[test]
    fn concurrency_deadline_error_represents_both_v1_retryable_causes() {
        #[derive(Clone, Copy)]
        enum Cause {
            LogicalConflictDeadline,
            ReevaluationBudgetExhausted,
        }

        const fn public_error_for(cause: Cause) -> PublicError {
            match cause {
                Cause::LogicalConflictDeadline | Cause::ReevaluationBudgetExhausted => {
                    PublicError::concurrency_deadline_exceeded()
                }
            }
        }

        for cause in [
            Cause::LogicalConflictDeadline,
            Cause::ReevaluationBudgetExhausted,
        ] {
            let error = public_error_for(cause);
            assert_eq!(error.kind(), PublicErrorKind::ConcurrencyDeadlineExceeded);
            assert_eq!(error.kind().code(), "concurrency_deadline_exceeded");
            assert_eq!(error.kind().safe_message(), "concurrency deadline exceeded");
            assert_eq!(error.kind().class(), ErrorClass::DeadlineExceeded);
            assert_eq!(error.kind().recovery_action(), RecoveryAction::Retry);
            assert_eq!(error.details(), &PublicErrorDetails::None);
        }
    }

    #[test]
    fn validation_codes_and_messages_are_stable_and_safe() {
        let expected = [
            ("missing_required_value", "required value is missing"),
            ("type_mismatch", "value has the wrong type"),
            ("invalid_value", "value is invalid"),
            ("out_of_range", "value is outside the allowed range"),
            ("too_long", "value exceeds the allowed length"),
            ("too_many_items", "collection has too many items"),
            (
                "unknown_field",
                "field is not defined by the active contract",
            ),
            ("duplicate_field", "field occurs more than once"),
        ];

        for (code, (stable_code, stable_message)) in VALIDATION_CODES.into_iter().zip(expected) {
            assert_eq!(code.code(), stable_code);
            assert_eq!(code.safe_message(), stable_message);
            assert!(stable_code.is_ascii());
            assert!(stable_message.is_ascii());
        }
    }

    #[test]
    fn validation_paths_are_structured_and_bounded() {
        let boundary =
            vec![
                ValidationPathSegment::Field(FieldId::new(1).expect("field ID is nonzero"));
                MAX_VALIDATION_PATH_SEGMENTS
            ];
        let path = ValidationPath::new(boundary).expect("boundary is accepted");
        assert_eq!(path.segments().len(), MAX_VALIDATION_PATH_SEGMENTS);
        assert!(ValidationPath::root().segments().is_empty());

        let above_boundary =
            vec![ValidationPathSegment::ListIndex(0); MAX_VALIDATION_PATH_SEGMENTS + 1];
        assert_eq!(
            ValidationPath::new(above_boundary),
            Err(ValidationPathError::TooManySegments)
        );
    }

    #[test]
    fn validation_issue_collections_are_non_empty_and_bounded() {
        assert_eq!(
            ValidationIssues::new(Vec::new()),
            Err(ValidationIssuesError::Empty)
        );

        let issues = ValidationIssues::new(vec![issue(); MAX_VALIDATION_ISSUES])
            .expect("boundary is accepted");
        assert_eq!(issues.as_slice().len(), MAX_VALIDATION_ISSUES);

        assert_eq!(
            ValidationIssues::new(vec![issue(); MAX_VALIDATION_ISSUES + 1]),
            Err(ValidationIssuesError::TooManyIssues)
        );
    }

    #[test]
    fn named_constructors_enforce_kind_and_detail_consistency() {
        let validation = PublicError::validation(ValidationIssues::one(issue()));
        assert_eq!(validation.kind(), PublicErrorKind::Validation);
        assert!(matches!(
            validation.details(),
            PublicErrorDetails::Validation(issues) if issues.as_slice().len() == 1
        ));

        let active_version = ContractVersion::new(41).expect("contract version is nonzero");
        let mismatch = PublicError::contract_mismatch(active_version);
        assert_eq!(mismatch.kind(), PublicErrorKind::ContractMismatch);
        assert_eq!(
            mismatch.details(),
            &PublicErrorDetails::ContractMismatch {
                active_contract_version: active_version,
            }
        );

        let contextless = [
            PublicError::idempotency_key_reuse(),
            PublicError::authorization_denied(),
            PublicError::concurrency_deadline_exceeded(),
            PublicError::storage_unavailable(),
            PublicError::outcome_unknown(),
        ];
        let expected = [
            PublicErrorKind::IdempotencyKeyReuse,
            PublicErrorKind::AuthorizationDenied,
            PublicErrorKind::ConcurrencyDeadlineExceeded,
            PublicErrorKind::StorageUnavailable,
            PublicErrorKind::OutcomeUnknown,
        ];
        for (error, kind) in contextless.into_iter().zip(expected) {
            assert_eq!(error.kind(), kind);
            assert_eq!(error.details(), &PublicErrorDetails::None);
            assert_eq!(error.incident_id(), None);
        }

        let execution =
            PublicError::command_execution_failed(ExecutionFailureCode::ArithmeticFault);
        assert_eq!(execution.kind(), PublicErrorKind::CommandExecutionFailed);
        assert_eq!(
            execution.details(),
            &PublicErrorDetails::CommandExecutionFailed {
                code: ExecutionFailureCode::ArithmeticFault,
            }
        );
        assert_eq!(execution.incident_id(), None);
    }

    #[test]
    fn public_error_never_exposes_a_source() {
        let error = PublicError::storage_unavailable();

        assert!(error.source().is_none());
    }

    #[test]
    fn internal_conversion_redacts_secret_source_everywhere_public() {
        let internal = InternalError::new(incident_id(), SecretSource);
        assert!(internal.source().is_some());
        assert!(!internal.to_string().contains("secret-canary"));
        assert!(!format!("{internal:?}").contains("SecretSource"));
        assert_eq!(
            internal.source().map(ToString::to_string).as_deref(),
            Some("secret-canary-internal-context")
        );

        let public = PublicError::from(internal);
        let display = public.to_string();
        let debug = format!("{public:?}");

        assert_eq!(public.kind(), PublicErrorKind::InternalDefect);
        assert_eq!(public.details(), &PublicErrorDetails::None);
        assert_eq!(public.incident_id(), Some(&incident_id()));
        assert!(public.source().is_none());
        assert!(!display.contains("secret-canary"));
        assert!(!debug.contains("secret-canary"));
        assert!(!display.contains("SecretSource"));
        assert!(!debug.contains("SecretSource"));
    }

    #[test]
    fn validation_details_cannot_carry_secret_canaries() {
        let error = PublicError::validation(ValidationIssues::one(issue()));
        let display = error.to_string();
        let debug = format!("{error:?}");

        assert!(!display.contains("secret-canary"));
        assert!(!debug.contains("secret-canary"));
        assert_eq!(display, "validation_failed: request validation failed");
    }

    #[test]
    fn public_display_contains_only_stable_fields() {
        let error = PublicError::internal_defect(incident_id());

        assert_eq!(
            error.to_string(),
            "internal_defect: an internal error occurred (incident 42424242-4242-7242-8242-424242424242)"
        );
    }

    /// ADR-0118 sweep anchor: the public error surface is structurally
    /// incapable of echoing a secret-classified field's VALUE, because no
    /// details shape carries caller or stored text at all — locations are
    /// stable IDs, messages are `&'static str`. This match is exhaustive on
    /// purpose: a new details variant carrying dynamic payload must red here
    /// and be reviewed against the secret-field guarantee before it ships.
    #[test]
    fn public_error_details_shapes_carry_no_dynamic_payload_for_secret_fields() {
        let error = PublicError::validation(ValidationIssues::one(issue()));
        // The channel is live: the diagnostic names the field's stable ID…
        let PublicErrorDetails::Validation(issues) = error.details() else {
            panic!("validation details expected");
        };
        assert_eq!(
            issues.as_slice()[0].path().segments()[0],
            ValidationPathSegment::Field(FieldId::new(7).expect("nonzero field"))
        );
        // …while every details shape is closed over value-free payloads.
        for details in [
            PublicErrorDetails::None,
            error.details().clone(),
            PublicErrorDetails::ContractMismatch {
                active_contract_version: ContractVersion::new(3).expect("nonzero version"),
            },
            PublicErrorDetails::CommandExecutionFailed {
                code: ExecutionFailureCode::ArithmeticFault,
            },
        ] {
            match details {
                PublicErrorDetails::None => {}
                PublicErrorDetails::Validation(issues) => {
                    for issue in issues.as_slice() {
                        for segment in issue.path().segments() {
                            match segment {
                                ValidationPathSegment::Field(_)
                                | ValidationPathSegment::ListIndex(_) => {}
                            }
                        }
                    }
                }
                PublicErrorDetails::ContractMismatch {
                    active_contract_version: _,
                } => {}
                PublicErrorDetails::CommandExecutionFailed { code: _ } => {}
            }
        }
    }

    #[test]
    fn incident_source_is_synchronous_fallible_and_has_no_fallback() {
        struct FixedSource(IncidentId);

        impl IncidentIdSource for FixedSource {
            fn next_incident_id(&self) -> Result<IncidentId, IncidentIdSourceError> {
                Ok(self.0)
            }
        }

        struct FailingSource;

        impl IncidentIdSource for FailingSource {
            fn next_incident_id(&self) -> Result<IncidentId, IncidentIdSourceError> {
                Err(IncidentIdSourceError)
            }
        }

        assert_eq!(
            FixedSource(incident_id()).next_incident_id(),
            Ok(incident_id())
        );
        assert_eq!(FailingSource.next_incident_id(), Err(IncidentIdSourceError));
        assert_eq!(
            IncidentIdSourceError.to_string(),
            "incident identifier source failed"
        );
    }

    #[test]
    fn emergency_internal_failure_has_no_identifier_or_disclosive_source() {
        let failure = EmergencyInternalFailure::from(IncidentIdSourceError);

        assert_eq!(failure.safe_message(), "an internal error occurred");
        assert_eq!(failure.class(), ErrorClass::Internal);
        assert_eq!(failure.recovery_action(), RecoveryAction::ContactOperator);
        assert_eq!(failure.to_string(), "an internal error occurred");
        assert_eq!(format!("{failure:?}"), "EmergencyInternalFailure");
        assert!(failure.source().is_none());
        assert!(!failure.to_string().contains("incident"));
    }
}
