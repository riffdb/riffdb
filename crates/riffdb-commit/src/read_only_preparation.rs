//! Exact proof joins for one unjournaled read-only command invocation.

use std::{error::Error, fmt, time::Instant};

use riffdb_catalog::ResolvedExecutablePlan;
use riffdb_conflict::CancellationToken;
use riffdb_contract_ir::ExecutionClass;
use riffdb_invariant::InputDerivedCommandFacts;
use riffdb_policy::{AuthorizedCommandExecution, CommandExecutionClass};
use riffdb_types::{
    CanonicalRecord, CommandId, DatabaseId, Environment, RequestId, ServiceIngressKindV1,
};

use crate::command_preparation::CommandRequestControl;

/// Opaque safe failure when independently produced read-only proofs do not join.
///
/// The error deliberately does not identify the mismatched database, plan,
/// actor, input, partition, or execution class.
#[derive(Clone, Copy, Eq, PartialEq)]
pub struct ReadOnlyExecutionPreparationError {
    _private: (),
}

impl ReadOnlyExecutionPreparationError {
    const fn proof_mismatch() -> Self {
        Self { _private: () }
    }
}

impl fmt::Debug for ReadOnlyExecutionPreparationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ReadOnlyExecutionPreparationError([REDACTED])")
    }
}

impl fmt::Display for ReadOnlyExecutionPreparationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("read-only execution proofs are inconsistent")
    }
}

impl Error for ReadOnlyExecutionPreparationError {}

/// One move-only, exact read-only preparation accepted after fresh policy.
///
/// This value joins the trusted database boundary, the exact catalog-resolved
/// plan, schema-normalized input, input-derived locality and target facts, and
/// a fresh authorization proof. It contains no idempotency state and grants no
/// storage mutation, command-journal, provenance, or sequence authority.
///
/// ```compile_fail
/// use riffdb_commit::ReadOnlyExecutionPreparation;
///
/// fn cannot_duplicate(value: &ReadOnlyExecutionPreparation) {
///     let _: ReadOnlyExecutionPreparation =
///         <ReadOnlyExecutionPreparation as Clone>::clone(value);
/// }
/// ```
#[must_use = "a checked read-only preparation must be submitted or explicitly discarded"]
pub struct ReadOnlyExecutionPreparation {
    resolved_plan: ResolvedExecutablePlan,
    normalized_input: CanonicalRecord,
    input_facts: InputDerivedCommandFacts,
    authorization: AuthorizedCommandExecution,
    request_id: RequestId,
    ingress: ServiceIngressKindV1,
    control: CommandRequestControl,
}

impl ReadOnlyExecutionPreparation {
    /// Consumes and joins every proof needed by the read-only executor.
    ///
    /// `database_id` and `environment` must come from trusted process
    /// configuration. This function performs no clock read, storage access,
    /// cancellation check, runtime evaluation, or durable operation.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        database_id: DatabaseId,
        environment: &Environment,
        resolved_plan: ResolvedExecutablePlan,
        normalized_input: CanonicalRecord,
        input_facts: InputDerivedCommandFacts,
        authorization: AuthorizedCommandExecution,
        request_id: RequestId,
        ingress: ServiceIngressKindV1,
        control: CommandRequestControl,
    ) -> Result<Self, ReadOnlyExecutionPreparationError> {
        let reference = resolved_plan.reference();
        let plan = resolved_plan.plan();

        if plan.execution_class() != ExecutionClass::ReadOnly
            || plan.idempotency_input().is_some()
            || !input_facts.declared_conflict_keys().is_empty()
            || !input_facts.matches_command(plan, &normalized_input)
            || authorization.class() != CommandExecutionClass::ReadOnly
            || authorization.lineage() != reference.contract_lineage()
            || authorization.version() != reference.contract_version()
            || authorization.command_id() != reference.command_id()
            || authorization.database_id() != database_id
            || authorization.environment() != environment
            || authorization.partition().lineage() != reference.contract_lineage()
            || authorization.partition().partition_key() != input_facts.partition_key()
        {
            return Err(ReadOnlyExecutionPreparationError::proof_mismatch());
        }

        Ok(Self {
            resolved_plan,
            normalized_input,
            input_facts,
            authorization,
            request_id,
            ingress,
            control,
        })
    }

    pub(crate) fn telemetry_identity(&self) -> (CommandId, ServiceIngressKindV1) {
        (self.resolved_plan.reference().command_id(), self.ingress)
    }

    pub(crate) fn into_parts(self) -> ReadOnlyExecutionPreparationParts {
        let (deadline, cancellation) = self.control.into_parts();
        ReadOnlyExecutionPreparationParts {
            resolved_plan: self.resolved_plan,
            normalized_input: self.normalized_input,
            input_facts: self.input_facts,
            authorization: self.authorization,
            request_id: self.request_id,
            deadline,
            cancellation,
        }
    }
}

impl fmt::Debug for ReadOnlyExecutionPreparation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ReadOnlyExecutionPreparation([REDACTED])")
    }
}

pub(crate) struct ReadOnlyExecutionPreparationParts {
    pub(crate) resolved_plan: ResolvedExecutablePlan,
    pub(crate) normalized_input: CanonicalRecord,
    pub(crate) input_facts: InputDerivedCommandFacts,
    pub(crate) authorization: AuthorizedCommandExecution,
    pub(crate) request_id: RequestId,
    pub(crate) deadline: Instant,
    pub(crate) cancellation: CancellationToken,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn preparation_mismatch_is_static_and_redacted() {
        let error = ReadOnlyExecutionPreparationError::proof_mismatch();

        assert_eq!(
            format!("{error:?}"),
            "ReadOnlyExecutionPreparationError([REDACTED])"
        );
        assert_eq!(
            error.to_string(),
            "read-only execution proofs are inconsistent"
        );
        assert!(error.source().is_none());
    }
}
