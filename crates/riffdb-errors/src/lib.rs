#![forbid(unsafe_code)]

//! Transport-neutral, public-safe errors for RiffDB.
//!
//! Declared business outcomes are contract values and intentionally do not
//! appear in this crate. [`PublicError`] describes failures outside that
//! outcome algebra without retaining untrusted diagnostic text or internal
//! error sources.

use std::error::Error;
use std::fmt;

use riffdb_types::{ContractVersion, ExecutionFailureCode, FieldId, IncidentId};

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
            Self::ContractMismatch => ErrorClass::FailedPrecondition,
            Self::StorageUnavailable => ErrorClass::Unavailable,
            Self::OutcomeUnknown => ErrorClass::Uncertain,
            Self::InternalDefect => ErrorClass::Internal,
            Self::CommandExecutionFailed => ErrorClass::FailedPrecondition,
        }
    }

    /// Returns safe recovery guidance for this failure.
    #[must_use]
    pub const fn recovery_action(self) -> RecoveryAction {
        match self {
            Self::Validation | Self::IdempotencyKeyReuse => RecoveryAction::CorrectRequest,
            Self::AuthorizationDenied => RecoveryAction::ObtainPermission,
            Self::ConcurrencyDeadlineExceeded | Self::StorageUnavailable => RecoveryAction::Retry,
            Self::ContractMismatch => RecoveryAction::RefreshContract,
            Self::OutcomeUnknown => RecoveryAction::ResolveWithSameIdempotencyKey,
            Self::InternalDefect | Self::CommandExecutionFailed => RecoveryAction::ContactOperator,
        }
    }
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
}

impl PublicError {
    const fn contextless(kind: PublicErrorKind) -> Self {
        Self {
            kind,
            details: PublicErrorDetails::None,
            incident_id: None,
        }
    }

    /// Creates a bounded validation failure.
    #[must_use]
    pub const fn validation(issues: ValidationIssues) -> Self {
        Self {
            kind: PublicErrorKind::Validation,
            details: PublicErrorDetails::Validation(issues),
            incident_id: None,
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
        }
    }

    /// Creates a redacted internal-defect failure with its incident identifier.
    #[must_use]
    pub const fn internal_defect(incident_id: IncidentId) -> Self {
        Self {
            kind: PublicErrorKind::InternalDefect,
            details: PublicErrorDetails::None,
            incident_id: Some(incident_id),
        }
    }

    /// Adds an opaque incident identifier for trusted diagnostic correlation.
    #[must_use]
    pub const fn with_incident_id(mut self, incident_id: IncidentId) -> Self {
        self.incident_id = Some(incident_id);
        self
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
    use super::*;

    const KINDS: [PublicErrorKind; 9] = [
        PublicErrorKind::Validation,
        PublicErrorKind::IdempotencyKeyReuse,
        PublicErrorKind::AuthorizationDenied,
        PublicErrorKind::ConcurrencyDeadlineExceeded,
        PublicErrorKind::ContractMismatch,
        PublicErrorKind::StorageUnavailable,
        PublicErrorKind::OutcomeUnknown,
        PublicErrorKind::InternalDefect,
        PublicErrorKind::CommandExecutionFailed,
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
        ];

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
