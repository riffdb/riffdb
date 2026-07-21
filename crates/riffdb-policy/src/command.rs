//! Exact command authorization and actor/provenance binding.

use std::{error::Error, fmt};

use riffdb_types::{
    ActorId, ActorKind, AdmittedActorContext, CommandId, ContractLineage, ContractVersion,
    DatabaseId, Environment, ScopedPartitionV1, TenantScope,
};

use crate::provenance::admit_for_actor_kind;
use crate::{
    AgentSessionAdmissionPolicy, AuditClass, AuthorizedProvenanceClaims, CommandExecutionClass,
    Obligations, OperationRequest, OutputClassification, PartitionConstraint,
    UntrustedInvocationClaims,
};

/// Safe failure to consume an allow proof as command-execution authority.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CommandAuthorizationBindingError {
    /// The allow proof was not produced for an execute-command request.
    OperationMismatch,
    /// The retained command facts and authorization obligations disagree.
    ObligationMismatch,
}

impl fmt::Display for CommandAuthorizationBindingError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::OperationMismatch => "authorization proof is not for command execution",
            Self::ObligationMismatch => "command authorization obligations are inconsistent",
        })
    }
}

impl Error for CommandAuthorizationBindingError {}

/// Move-only proof binding one fresh allow decision to exact command authority.
///
/// The proof retains the command identity and class, exact lineage-scoped
/// partition, authorizer-proven database boundary, policy-resolved actor, and
/// only policy-admitted provenance claims. It contains no capability token,
/// durable record, storage handle, or protocol value. Capability identity
/// remains on the separate service-audit path.
///
/// ```compile_fail
/// use riffdb_policy::AuthorizedCommandExecution;
///
/// fn cannot_duplicate(value: &AuthorizedCommandExecution) {
///     let _: AuthorizedCommandExecution =
///         <AuthorizedCommandExecution as Clone>::clone(value);
/// }
/// ```
///
/// ```compile_fail
/// use riffdb_policy::AuthorizedCommandExecution;
///
/// fn cannot_fabricate() -> AuthorizedCommandExecution {
///     AuthorizedCommandExecution::default()
/// }
/// ```
#[derive(Eq, PartialEq)]
pub struct AuthorizedCommandExecution {
    database_id: DatabaseId,
    environment: Environment,
    lineage: ContractLineage,
    version: ContractVersion,
    command_id: CommandId,
    class: CommandExecutionClass,
    partition: ScopedPartitionV1,
    actor: AdmittedActorContext,
    provenance: AuthorizedProvenanceClaims,
}

impl AuthorizedCommandExecution {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn bind(
        database_id: DatabaseId,
        environment: Environment,
        request: OperationRequest,
        obligations: Obligations,
        principal_id: ActorId,
        actor_kind: ActorKind,
        claims: UntrustedInvocationClaims,
        agent_session_policy: AgentSessionAdmissionPolicy,
    ) -> Result<Self, CommandAuthorizationBindingError> {
        let command = request
            .into_command_execution()
            .ok_or(CommandAuthorizationBindingError::OperationMismatch)?;
        let expected_audit = match command.class {
            CommandExecutionClass::ReadOnly => None,
            CommandExecutionClass::Mutation => Some(AuditClass::CommandMutation),
        };
        let exact_partition = matches!(
            obligations.partition_constraint(),
            Some(PartitionConstraint::Exact(partition)) if partition == &command.partition
        );
        if obligations.effective_tenant_scope() != &TenantScope::Global
            || !exact_partition
            || obligations.field_mask().is_some()
            || obligations.row_limit().is_some()
            || obligations.audit_class() != expected_audit
            || obligations.output_classification()
                != OutputClassification::PolicyFilteredApplicationData
        {
            return Err(CommandAuthorizationBindingError::ObligationMismatch);
        }

        let admission = admit_for_actor_kind(actor_kind, claims, agent_session_policy);
        if obligations.validated_approval() != admission.provenance().approval_id() {
            return Err(CommandAuthorizationBindingError::ObligationMismatch);
        }
        let (provenance, agent_session_id) = admission.into_parts();
        Ok(Self {
            database_id,
            environment,
            lineage: command.lineage,
            version: command.version,
            command_id: command.command_id,
            class: command.class,
            partition: command.partition,
            actor: AdmittedActorContext::new(
                principal_id,
                actor_kind,
                obligations.effective_tenant_scope().clone(),
                agent_session_id,
            ),
            provenance,
        })
    }

    /// Returns the exact database boundary checked by the current authorizer.
    #[must_use]
    pub const fn database_id(&self) -> DatabaseId {
        self.database_id
    }

    /// Borrows the exact environment checked by the current authorizer.
    #[must_use]
    pub const fn environment(&self) -> &Environment {
        &self.environment
    }

    /// Borrows the exact authorized contract lineage.
    #[must_use]
    pub const fn lineage(&self) -> &ContractLineage {
        &self.lineage
    }

    /// Returns the exact authorized contract version.
    #[must_use]
    pub const fn version(&self) -> ContractVersion {
        self.version
    }

    /// Returns the exact authorized stable command identity.
    #[must_use]
    pub const fn command_id(&self) -> CommandId {
        self.command_id
    }

    /// Returns the authorized execution class derived from the checked plan.
    #[must_use]
    pub const fn class(&self) -> CommandExecutionClass {
        self.class
    }

    /// Borrows the exact lineage-scoped command partition.
    #[must_use]
    pub const fn partition(&self) -> &ScopedPartitionV1 {
        &self.partition
    }

    /// Borrows the actor bound to the same fresh policy decision.
    #[must_use]
    pub const fn actor(&self) -> &AdmittedActorContext {
        &self.actor
    }

    /// Borrows the provenance claims admitted for this exact binding.
    #[must_use]
    pub const fn provenance(&self) -> &AuthorizedProvenanceClaims {
        &self.provenance
    }
}

impl fmt::Debug for AuthorizedCommandExecution {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("AuthorizedCommandExecution([REDACTED])")
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU16;

    use riffdb_types::{
        AggregateTypeId, ApprovalId, EntityTypeId, FieldId, PartitionKey, PartitionKeyBuilder,
        TenantId,
    };

    use super::*;
    use crate::FieldMask;

    fn lineage() -> ContractLineage {
        ContractLineage::new("example.command-binding").expect("bounded lineage")
    }

    fn partition(value: u64) -> PartitionKey {
        let mut builder = PartitionKeyBuilder::new(AggregateTypeId::first());
        builder.push_u64(value).expect("bounded component");
        builder.finish().expect("complete partition")
    }

    fn scoped_partition(value: u64) -> ScopedPartitionV1 {
        ScopedPartitionV1::new(lineage(), partition(value))
    }

    fn request() -> OperationRequest {
        OperationRequest::execute_command(
            lineage(),
            ContractVersion::new(1).expect("nonzero version"),
            CommandId::first(),
            CommandExecutionClass::Mutation,
            partition(7),
        )
    }

    fn obligations(
        tenant: TenantScope,
        partition_constraint: Option<PartitionConstraint>,
        field_mask: Option<FieldMask>,
        row_limit: Option<NonZeroU16>,
        approval: Option<ApprovalId>,
        audit_class: Option<AuditClass>,
        output: OutputClassification,
    ) -> Obligations {
        Obligations::new(
            tenant,
            partition_constraint,
            field_mask,
            row_limit,
            approval,
            audit_class,
            output,
        )
    }

    fn bind(
        obligations: Obligations,
    ) -> Result<AuthorizedCommandExecution, CommandAuthorizationBindingError> {
        AuthorizedCommandExecution::bind(
            DatabaseId::from_unix_milliseconds_and_random(1, [0x61; 10]).expect("valid UUIDv7"),
            Environment::new("command-binding-test").expect("bounded environment"),
            request(),
            obligations,
            ActorId::new("authorized-agent").expect("bounded actor"),
            ActorKind::Agent,
            UntrustedInvocationClaims::new(None, None, None, None, None),
            AgentSessionAdmissionPolicy::Discard,
        )
    }

    #[test]
    fn every_inconsistent_command_obligation_fails_closed() {
        let cases = [
            obligations(
                TenantScope::Tenant(TenantId::new("tenant-a").expect("bounded tenant")),
                Some(PartitionConstraint::Exact(scoped_partition(7))),
                None,
                None,
                None,
                Some(AuditClass::CommandMutation),
                OutputClassification::PolicyFilteredApplicationData,
            ),
            obligations(
                TenantScope::Global,
                Some(PartitionConstraint::Exact(scoped_partition(8))),
                None,
                None,
                None,
                Some(AuditClass::CommandMutation),
                OutputClassification::PolicyFilteredApplicationData,
            ),
            obligations(
                TenantScope::Global,
                Some(PartitionConstraint::Exact(scoped_partition(7))),
                Some(FieldMask::new(
                    lineage(),
                    EntityTypeId::first(),
                    vec![FieldId::first()],
                )),
                None,
                None,
                Some(AuditClass::CommandMutation),
                OutputClassification::PolicyFilteredApplicationData,
            ),
            obligations(
                TenantScope::Global,
                Some(PartitionConstraint::Exact(scoped_partition(7))),
                None,
                Some(NonZeroU16::MIN),
                None,
                Some(AuditClass::CommandMutation),
                OutputClassification::PolicyFilteredApplicationData,
            ),
            obligations(
                TenantScope::Global,
                Some(PartitionConstraint::Exact(scoped_partition(7))),
                None,
                None,
                None,
                None,
                OutputClassification::PolicyFilteredApplicationData,
            ),
            obligations(
                TenantScope::Global,
                Some(PartitionConstraint::Exact(scoped_partition(7))),
                None,
                None,
                None,
                Some(AuditClass::CommandMutation),
                OutputClassification::PublicMetadata,
            ),
            obligations(
                TenantScope::Global,
                Some(PartitionConstraint::Exact(scoped_partition(7))),
                None,
                None,
                Some(ApprovalId::new("required-approval").expect("bounded approval")),
                Some(AuditClass::CommandMutation),
                OutputClassification::PolicyFilteredApplicationData,
            ),
        ];

        for obligations in cases {
            assert_eq!(
                bind(obligations),
                Err(CommandAuthorizationBindingError::ObligationMismatch)
            );
        }
    }
}
