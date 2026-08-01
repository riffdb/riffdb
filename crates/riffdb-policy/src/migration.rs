//! Closed, process-local authorization types for contract migration.

use std::{fmt, num::NonZeroU64};

use riffdb_types::{
    ActorId, ActorKind, CapabilityId, ContractLineage, ContractMigrationInputHash,
    ContractMigrationOperationId, DatabaseId, Environment,
};

use crate::{Obligations, PolicyCode};

/// The three public contract-migration actions known to policy.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ContractMigrationPolicyOperation {
    /// Run one complete read-only preflight.
    Check,
    /// Apply one exact checked migration.
    Apply,
    /// Observe one caller-stable operation.
    GetOperation,
}

/// Checked policy facts for one contract-migration authorization safe point.
#[derive(Clone, Eq, PartialEq)]
pub struct ContractMigrationAuthorizationRequest {
    operation_id: ContractMigrationOperationId,
    operation: ContractMigrationPolicyOperation,
    lineage: ContractLineage,
    input_hash: Option<ContractMigrationInputHash>,
}

impl ContractMigrationAuthorizationRequest {
    /// Constructs one exact start authorization request.
    #[must_use]
    pub const fn start(
        operation_id: ContractMigrationOperationId,
        operation: ContractMigrationPolicyOperation,
        lineage: ContractLineage,
        input_hash: ContractMigrationInputHash,
    ) -> Self {
        Self {
            operation_id,
            operation,
            lineage,
            input_hash: Some(input_hash),
        }
    }

    /// Constructs one protected observation authorization request.
    #[must_use]
    pub const fn get_operation(
        operation_id: ContractMigrationOperationId,
        lineage: ContractLineage,
    ) -> Self {
        Self {
            operation_id,
            operation: ContractMigrationPolicyOperation::GetOperation,
            lineage,
            input_hash: None,
        }
    }

    /// Returns the caller-stable operation identity.
    #[must_use]
    pub const fn operation_id(&self) -> ContractMigrationOperationId {
        self.operation_id
    }

    /// Returns the exact action.
    #[must_use]
    pub const fn operation(&self) -> ContractMigrationPolicyOperation {
        self.operation
    }

    /// Borrows the exact contract lineage.
    #[must_use]
    pub const fn lineage(&self) -> &ContractLineage {
        &self.lineage
    }

    /// Returns the start input identity, absent only for observation.
    #[must_use]
    pub const fn input_hash(&self) -> Option<ContractMigrationInputHash> {
        self.input_hash
    }
}

impl fmt::Debug for ContractMigrationAuthorizationRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ContractMigrationAuthorizationRequest([REDACTED])")
    }
}

/// Move-only proof for one exact contract-migration safe point.
#[derive(Eq, PartialEq)]
pub struct AuthorizedContractMigration {
    database_id: DatabaseId,
    environment: Environment,
    request: ContractMigrationAuthorizationRequest,
    obligations: Obligations,
    capability_id: CapabilityId,
    capability_revision: NonZeroU64,
    principal_id: ActorId,
    actor_kind: ActorKind,
}

impl AuthorizedContractMigration {
    #[allow(clippy::too_many_arguments)]
    pub(crate) const fn new(
        database_id: DatabaseId,
        environment: Environment,
        request: ContractMigrationAuthorizationRequest,
        obligations: Obligations,
        capability_id: CapabilityId,
        capability_revision: NonZeroU64,
        principal_id: ActorId,
        actor_kind: ActorKind,
    ) -> Self {
        Self {
            database_id,
            environment,
            request,
            obligations,
            capability_id,
            capability_revision,
            principal_id,
            actor_kind,
        }
    }

    /// Returns the checked database.
    #[must_use]
    pub const fn database_id(&self) -> DatabaseId {
        self.database_id
    }
    /// Borrows the checked environment.
    #[must_use]
    pub const fn environment(&self) -> &Environment {
        &self.environment
    }
    /// Borrows the exact checked action.
    #[must_use]
    pub const fn request(&self) -> &ContractMigrationAuthorizationRequest {
        &self.request
    }
    /// Borrows the ordinary obligations.
    #[must_use]
    pub const fn obligations(&self) -> &Obligations {
        &self.obligations
    }
    /// Returns the current capability identity.
    #[must_use]
    pub const fn authorizing_capability_id(&self) -> CapabilityId {
        self.capability_id
    }
    /// Returns the current capability revision.
    #[must_use]
    pub const fn authorizing_capability_revision(&self) -> NonZeroU64 {
        self.capability_revision
    }
    /// Borrows the admitted principal.
    #[must_use]
    pub const fn principal_id(&self) -> &ActorId {
        &self.principal_id
    }
    /// Returns the actor kind.
    #[must_use]
    pub const fn actor_kind(&self) -> ActorKind {
        self.actor_kind
    }
}

impl fmt::Debug for AuthorizedContractMigration {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("AuthorizedContractMigration([REDACTED])")
    }
}

/// Deny-by-default contract-migration policy result.
#[derive(Eq, PartialEq)]
pub enum ContractMigrationDecision {
    /// Current policy allowed the exact action.
    Allow(Box<AuthorizedContractMigration>),
    /// Current policy denied with a closed code.
    Deny(PolicyCode),
}

impl fmt::Debug for ContractMigrationDecision {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Allow(_) => formatter.write_str("ContractMigrationDecision::Allow([REDACTED])"),
            Self::Deny(code) => formatter
                .debug_tuple("ContractMigrationDecision::Deny")
                .field(code)
                .finish(),
        }
    }
}
