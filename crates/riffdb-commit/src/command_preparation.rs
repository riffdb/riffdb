//! Exact proof joins before command admission enters the coordinator actor.

use std::{error::Error, fmt, time::Instant};

use riffdb_catalog::ResolvedExecutablePlan;
use riffdb_conflict::CancellationToken;
use riffdb_contract_ir::ExecutionClass;
use riffdb_idempotency::PreparedIdempotencyRecheckV1;
use riffdb_invariant::InputDerivedCommandFacts;
use riffdb_policy::{AuthorizedCommandExecution, CommandExecutionClass};
use riffdb_types::{
    CanonicalRecord, CanonicalValue, CommandId, DatabaseId, Environment, RequestId,
    ServiceAuditLinkV1, ServiceAuditPhaseV1, ServiceIngressKindV1, ServiceOperationV1,
};

use crate::AdministrationAuditInputView;

const QUEUED_PREPARATION_FIXED_BYTES: usize = 4 * 1_024;
const QUEUED_BYTE_UNIT: usize = 1_024;

/// Closed public-safe result when current policy no longer authorizes a
/// speculatively evaluated command.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PostEvaluationAuthorizationError {
    /// Current policy made an explicit deny decision.
    Denied,
    /// Current policy state could not be loaded or evaluated.
    Unavailable,
    /// The returned decision could not bind to the exact command facts.
    Integrity,
}

/// One-use current-policy safe point invoked after deterministic evaluation.
///
/// Implementations may read current authorization state but must not mutate
/// application data. The returned proof is consumed and compared with the
/// command's frozen semantic identity before the authoritative write opens.
pub trait PostEvaluationCommandAuthorizer: Send {
    /// Produces a fresh exact command-execution proof or fails closed.
    fn authorize(&self) -> Result<AuthorizedCommandExecution, PostEvaluationAuthorizationError>;
}

/// Queued retained-byte units charged for one normalized command input.
///
/// Service admission acquires this budget before authorization; submit asserts
/// the preparation does not need more units than were pre-admitted.
#[must_use]
pub fn queued_preparation_units(input: &CanonicalRecord) -> u32 {
    let retained = QUEUED_PREPARATION_FIXED_BYTES
        .saturating_add(canonical_record_retained_bytes(input))
        .div_ceil(QUEUED_BYTE_UNIT)
        .max(1);
    u32::try_from(retained).unwrap_or(u32::MAX)
}

fn canonical_record_retained_bytes(record: &CanonicalRecord) -> usize {
    record.fields().iter().fold(0usize, |total, (_, value)| {
        total.saturating_add(8).saturating_add(match value {
            CanonicalValue::String(value) => value.len(),
            CanonicalValue::Bytes(value) => value.len(),
            CanonicalValue::List(values) => values.values().iter().fold(0usize, |total, value| {
                total.saturating_add(canonical_value_retained_bytes(value))
            }),
            CanonicalValue::Record(record) => canonical_record_retained_bytes(record),
            CanonicalValue::Null
            | CanonicalValue::Bool(_)
            | CanonicalValue::I64(_)
            | CanonicalValue::U64(_)
            | CanonicalValue::Decimal(_)
            | CanonicalValue::Money(_)
            | CanonicalValue::Timestamp(_)
            | CanonicalValue::Date(_)
            | CanonicalValue::Uuid(_)
            | CanonicalValue::Enum { .. } => 32,
        })
    })
}

fn canonical_value_retained_bytes(value: &CanonicalValue) -> usize {
    match value {
        CanonicalValue::String(value) => value.len(),
        CanonicalValue::Bytes(value) => value.len(),
        CanonicalValue::List(values) => values.values().iter().fold(0usize, |total, value| {
            total.saturating_add(canonical_value_retained_bytes(value))
        }),
        CanonicalValue::Record(record) => canonical_record_retained_bytes(record),
        CanonicalValue::Null
        | CanonicalValue::Bool(_)
        | CanonicalValue::I64(_)
        | CanonicalValue::U64(_)
        | CanonicalValue::Decimal(_)
        | CanonicalValue::Money(_)
        | CanonicalValue::Timestamp(_)
        | CanonicalValue::Date(_)
        | CanonicalValue::Uuid(_)
        | CanonicalValue::Enum { .. } => 32,
    }
}

/// Cloneable process-local authority to cancel one command request.
///
/// This commit-owned handle prevents application services and transports from
/// depending on conflict-manager types. It can request cancellation but cannot
/// observe internal waiter or acquisition state.
#[derive(Clone)]
pub struct CommandCancellationHandle {
    cancellation: CancellationToken,
}

impl CommandCancellationHandle {
    /// Requests monotonic cancellation of the associated command.
    pub fn cancel(&self) {
        self.cancellation.cancel();
    }
}

impl fmt::Debug for CommandCancellationHandle {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("CommandCancellationHandle([REDACTED])")
    }
}

/// Process-local controls carried unchanged to command acquisition.
///
/// Construction records caller-supplied process state only. A past deadline or
/// already-cancelled token remains a valid control; the later acquisition
/// boundary decides whether work may proceed. The value is move-only so the
/// coordinator receives one exact control together with one preparation.
///
/// ```compile_fail
/// use riffdb_commit::CommandRequestControl;
///
/// fn cannot_duplicate(value: &CommandRequestControl) {
///     let _: CommandRequestControl = <CommandRequestControl as Clone>::clone(value);
/// }
/// ```
pub struct CommandRequestControl {
    deadline: Instant,
    cancellation: CancellationToken,
}

impl CommandRequestControl {
    /// Creates an exact control and its only service-facing cancellation type.
    ///
    /// The returned handle may be cloned across process tasks. The control
    /// itself remains move-only and retains the conflict-manager signal
    /// privately for later acquisition.
    #[must_use]
    pub fn new(deadline: Instant) -> (Self, CommandCancellationHandle) {
        let cancellation = CancellationToken::new();
        let handle = CommandCancellationHandle {
            cancellation: cancellation.clone(),
        };
        (
            Self {
                deadline,
                cancellation,
            },
            handle,
        )
    }

    #[allow(dead_code)] // Consumed by the next coordinator admission slice.
    pub(crate) fn into_parts(self) -> (Instant, CancellationToken) {
        (self.deadline, self.cancellation)
    }
}

impl fmt::Debug for CommandRequestControl {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("CommandRequestControl([REDACTED])")
    }
}

/// Opaque safe failure when independently produced command proofs do not join.
///
/// The private field prevents fabrication and the diagnostic deliberately does
/// not reveal which identity, scope, input, class, or partition comparison
/// failed.
#[derive(Clone, Copy, Eq, PartialEq)]
pub struct CommandExecutionPreparationError {
    _private: (),
}

impl CommandExecutionPreparationError {
    const fn proof_mismatch() -> Self {
        Self { _private: () }
    }
}

impl fmt::Debug for CommandExecutionPreparationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("CommandExecutionPreparationError([REDACTED])")
    }
}

impl fmt::Display for CommandExecutionPreparationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("command execution proofs are inconsistent")
    }
}

impl Error for CommandExecutionPreparationError {}

/// One move-only, exact command preparation ready for coordinator admission.
///
/// The constructor is the sole public join between catalog resolution,
/// idempotency inspection, pure input-derived facts, and a fresh policy allow.
/// Retained values have no public accessors or serialization surface and can be
/// consumed only by commit-owned orchestration.
///
/// ```compile_fail
/// use riffdb_commit::CommandExecutionPreparation;
///
/// fn cannot_duplicate(value: &CommandExecutionPreparation) {
///     let _: CommandExecutionPreparation =
///         <CommandExecutionPreparation as Clone>::clone(value);
/// }
/// ```
#[must_use = "a checked command preparation must be submitted or explicitly discarded"]
pub struct CommandExecutionPreparation {
    resolved_plan: ResolvedExecutablePlan,
    normalized_input: CanonicalRecord,
    idempotency: PreparedIdempotencyRecheckV1,
    input_facts: InputDerivedCommandFacts,
    authorization: AuthorizedCommandExecution,
    request_id: RequestId,
    ingress: ServiceIngressKindV1,
    control: CommandRequestControl,
    audited_lifecycle: Option<AuditedCommandLifecycle>,
    post_evaluation_authorizer: Option<Box<dyn PostEvaluationCommandAuthorizer>>,
}

pub(crate) struct AuditedCommandLifecycle {
    pub(crate) started: Box<dyn AdministrationAuditInputView>,
    pub(crate) release: CompleteOutcomeReleaseProof,
}

/// Private compiler-and-policy-owned proof that a successful command response
/// needs no post-commit policy or size decision.
pub(crate) struct CompleteOutcomeReleaseProof {
    _private: (),
}

impl CommandExecutionPreparation {
    /// Consumes and joins every proof required before mutation admission.
    ///
    /// `database_id` and `environment` must come from trusted database process
    /// configuration. Tenant and principal scope are taken only from the exact
    /// authorization proof; no independently supplied actor identity is
    /// accepted. This function performs no clock read, cancellation check,
    /// storage access, actor submission, or durable mutation.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        database_id: DatabaseId,
        environment: &Environment,
        resolved_plan: ResolvedExecutablePlan,
        normalized_input: CanonicalRecord,
        idempotency: PreparedIdempotencyRecheckV1,
        input_facts: InputDerivedCommandFacts,
        authorization: AuthorizedCommandExecution,
        request_id: RequestId,
        ingress: ServiceIngressKindV1,
        control: CommandRequestControl,
    ) -> Result<Self, CommandExecutionPreparationError> {
        let reference = resolved_plan.reference();
        let plan = resolved_plan.plan();
        let Some(idempotency_field) = plan.idempotency_input() else {
            return Err(CommandExecutionPreparationError::proof_mismatch());
        };

        if !idempotency.matches_preparation(reference, &normalized_input, idempotency_field) {
            return Err(CommandExecutionPreparationError::proof_mismatch());
        }
        if !input_facts.matches_command(plan, &normalized_input) {
            return Err(CommandExecutionPreparationError::proof_mismatch());
        }
        if authorization.lineage() != reference.contract_lineage()
            || authorization.version() != reference.contract_version()
            || authorization.command_id() != reference.command_id()
            || authorization.database_id() != database_id
            || authorization.environment() != environment
        {
            return Err(CommandExecutionPreparationError::proof_mismatch());
        }
        if plan.execution_class() != ExecutionClass::IdempotentMutation
            || authorization.class() != CommandExecutionClass::Mutation
        {
            return Err(CommandExecutionPreparationError::proof_mismatch());
        }
        if authorization.partition().lineage() != reference.contract_lineage()
            || authorization.partition().partition_key() != input_facts.partition_key()
        {
            return Err(CommandExecutionPreparationError::proof_mismatch());
        }
        if !idempotency.matches_scope(
            database_id,
            environment,
            authorization.actor().tenant_scope(),
            authorization.actor().principal_id(),
            reference.contract_lineage(),
            reference.command_id(),
        ) {
            return Err(CommandExecutionPreparationError::proof_mismatch());
        }

        Ok(Self {
            resolved_plan,
            normalized_input,
            idempotency,
            input_facts,
            authorization,
            request_id,
            ingress,
            control,
            audited_lifecycle: None,
            post_evaluation_authorizer: None,
        })
    }

    /// Attaches the exact service start record after the final policy allow.
    ///
    /// The complete-outcome proof is minted only from the same resolved plan
    /// and authorization already sealed into this preparation.
    pub fn with_audited_lifecycle(
        mut self,
        started: Box<dyn AdministrationAuditInputView>,
    ) -> Result<Self, CommandExecutionPreparationError> {
        if self.audited_lifecycle.is_some()
            || started.request_id() != &self.request_id
            || started.operation() != &ServiceOperationV1::ExecuteCommand
            || started.phase() != &ServiceAuditPhaseV1::Started
            || started.link() != &ServiceAuditLinkV1::None
            || started.principal_id() != self.authorization.actor().principal_id()
            || *started.actor_kind() != self.authorization.actor().actor_kind()
            || started.approval_id() != self.authorization.provenance().approval_id()
            || self.resolved_plan.plan().outcomes().is_empty()
        {
            return Err(CommandExecutionPreparationError::proof_mismatch());
        }
        self.audited_lifecycle = Some(AuditedCommandLifecycle {
            started,
            release: CompleteOutcomeReleaseProof { _private: () },
        });
        Ok(self)
    }

    /// Attaches the mandatory post-evaluation current-policy safe point for the
    /// audited application mutation path.
    pub fn with_post_evaluation_authorizer(
        mut self,
        authorizer: Box<dyn PostEvaluationCommandAuthorizer>,
    ) -> Result<Self, CommandExecutionPreparationError> {
        if self.post_evaluation_authorizer.is_some() || self.audited_lifecycle.is_none() {
            return Err(CommandExecutionPreparationError::proof_mismatch());
        }
        self.post_evaluation_authorizer = Some(authorizer);
        Ok(self)
    }

    pub(crate) fn telemetry_identity(&self) -> (CommandId, ServiceIngressKindV1) {
        (self.resolved_plan.reference().command_id(), self.ingress)
    }

    pub(crate) fn queued_byte_units(&self) -> u32 {
        queued_preparation_units(&self.normalized_input)
    }

    #[allow(dead_code)] // Consumed by the next coordinator admission slice.
    pub(crate) fn into_parts(self) -> CommandExecutionPreparationParts {
        let (deadline, cancellation) = self.control.into_parts();
        CommandExecutionPreparationParts {
            resolved_plan: self.resolved_plan,
            normalized_input: self.normalized_input,
            idempotency: self.idempotency,
            input_facts: self.input_facts,
            authorization: self.authorization,
            request_id: self.request_id,
            deadline,
            cancellation,
            audited_lifecycle: self.audited_lifecycle,
            post_evaluation_authorizer: self.post_evaluation_authorizer,
        }
    }
}

impl fmt::Debug for CommandExecutionPreparation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("CommandExecutionPreparation([REDACTED])")
    }
}

#[allow(dead_code)] // Private handoff shape for the next coordinator admission slice.
pub(crate) struct CommandExecutionPreparationParts {
    pub(crate) resolved_plan: ResolvedExecutablePlan,
    pub(crate) normalized_input: CanonicalRecord,
    pub(crate) idempotency: PreparedIdempotencyRecheckV1,
    pub(crate) input_facts: InputDerivedCommandFacts,
    pub(crate) authorization: AuthorizedCommandExecution,
    pub(crate) request_id: RequestId,
    pub(crate) deadline: Instant,
    pub(crate) cancellation: CancellationToken,
    pub(crate) audited_lifecycle: Option<AuditedCommandLifecycle>,
    pub(crate) post_evaluation_authorizer: Option<Box<dyn PostEvaluationCommandAuthorizer>>,
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use riffdb_types::FieldId;

    use super::*;

    #[test]
    fn cloned_public_handle_cancels_the_private_control_token() {
        let deadline = Instant::now()
            .checked_sub(Duration::from_secs(1))
            .expect("representable past deadline");
        let (control, handle) = CommandRequestControl::new(deadline);
        let cloned = handle.clone();

        cloned.cancel();

        let (retained_deadline, private_token) = control.into_parts();
        assert_eq!(retained_deadline, deadline);
        assert!(private_token.is_cancelled());
        assert_eq!(
            format!("{handle:?}"),
            "CommandCancellationHandle([REDACTED])"
        );
    }

    #[test]
    fn queued_preparation_charge_is_independent_of_item_capacity_and_rounds_up() {
        let empty = CanonicalRecord::new(Vec::new()).expect("empty canonical record");
        assert_eq!(queued_preparation_units(&empty), 4);

        let with_string = CanonicalRecord::new(vec![(
            FieldId::new(1).expect("field ID"),
            CanonicalValue::string("x".repeat(2 * 1_024)).expect("bounded string"),
        )])
        .expect("canonical record");
        assert_eq!(
            queued_preparation_units(&with_string),
            7,
            "four fixed KiB plus the field identity and payload round up independently"
        );
    }
}
