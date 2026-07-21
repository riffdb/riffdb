//! Exact proof joins before command admission enters the coordinator actor.

use std::{error::Error, fmt, time::Instant};

use riffdb_catalog::ResolvedExecutablePlan;
use riffdb_conflict::CancellationToken;
use riffdb_contract_ir::ExecutionClass;
use riffdb_idempotency::PreparedIdempotencyRecheckV1;
use riffdb_invariant::InputDerivedCommandFacts;
use riffdb_policy::{AuthorizedCommandExecution, CommandExecutionClass};
use riffdb_types::{CanonicalRecord, DatabaseId, Environment, RequestId};

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
    control: CommandRequestControl,
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
            control,
        })
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
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

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
}
