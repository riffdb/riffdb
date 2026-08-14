//! Closed, process-local authorization types for offline maintenance.

use std::fmt;
use std::num::NonZeroU64;

use riffdb_types::{
    ActorId, ActorKind, CapabilityId, DatabaseId, Environment, OfflineMaintenanceInputHash,
    OfflineMaintenanceOperationId, OfflineMaintenanceOperationKind,
};

use crate::{Obligations, PolicyCode};

/// The four public offline-maintenance actions known to policy.
///
/// This is a process-local policy registry. It deliberately has no stable tag
/// or serialization API and is not a [`riffdb_types::ServiceOperationV1`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OfflineMaintenancePolicyOperation {
    /// Start one exact shared maintenance operation kind.
    Start(OfflineMaintenanceOperationKind),
    /// Observe one caller-stable maintenance operation.
    GetOperation,
}

/// Checked policy facts for one offline-maintenance authorization safe point.
#[derive(Clone, Eq, PartialEq)]
pub struct OfflineMaintenanceAuthorizationRequest {
    operation_id: OfflineMaintenanceOperationId,
    operation: OfflineMaintenancePolicyOperation,
    input_hash: Option<OfflineMaintenanceInputHash>,
}

impl OfflineMaintenanceAuthorizationRequest {
    /// Constructs authorization facts for offline backup creation.
    #[must_use]
    pub const fn create_backup(
        operation_id: OfflineMaintenanceOperationId,
        input_hash: OfflineMaintenanceInputHash,
    ) -> Self {
        Self {
            operation_id,
            operation: OfflineMaintenancePolicyOperation::Start(
                OfflineMaintenanceOperationKind::CreateBackup,
            ),
            input_hash: Some(input_hash),
        }
    }

    /// Constructs authorization facts for offline backup restore.
    #[must_use]
    pub const fn restore_backup(
        operation_id: OfflineMaintenanceOperationId,
        input_hash: OfflineMaintenanceInputHash,
    ) -> Self {
        Self {
            operation_id,
            operation: OfflineMaintenancePolicyOperation::Start(
                OfflineMaintenanceOperationKind::RestoreBackup,
            ),
            input_hash: Some(input_hash),
        }
    }

    /// Constructs authorization facts for immutable-backup retirement.
    #[must_use]
    pub const fn retire_backup(
        operation_id: OfflineMaintenanceOperationId,
        input_hash: OfflineMaintenanceInputHash,
    ) -> Self {
        Self {
            operation_id,
            operation: OfflineMaintenancePolicyOperation::Start(
                OfflineMaintenanceOperationKind::RetireBackup,
            ),
            input_hash: Some(input_hash),
        }
    }

    /// Constructs authorization facts for protected operation polling.
    #[must_use]
    pub const fn get_operation(operation_id: OfflineMaintenanceOperationId) -> Self {
        Self {
            operation_id,
            operation: OfflineMaintenancePolicyOperation::GetOperation,
            input_hash: None,
        }
    }

    /// Returns the exact caller-stable receipt operation being authorized.
    #[must_use]
    pub const fn operation_id(&self) -> OfflineMaintenanceOperationId {
        self.operation_id
    }

    /// Returns the exact process-local action being authorized.
    #[must_use]
    pub const fn operation(&self) -> OfflineMaintenancePolicyOperation {
        self.operation
    }

    /// Returns the canonical semantic input identity for a start request.
    ///
    /// Polling has no new semantic input and therefore returns `None`.
    #[must_use]
    pub const fn input_hash(&self) -> Option<OfflineMaintenanceInputHash> {
        self.input_hash
    }
}

impl fmt::Debug for OfflineMaintenanceAuthorizationRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("OfflineMaintenanceAuthorizationRequest([REDACTED])")
    }
}

/// Privately constructed proof for one exact offline-maintenance safe point.
///
/// The proof is deliberately non-`Clone` and nonserializable. A healthy
/// current-database proof cannot stand in for the independently authenticated
/// and authorized staged-database proof required by restore.
#[derive(Eq, PartialEq)]
pub struct AuthorizedOfflineMaintenance {
    database_id: DatabaseId,
    environment: Environment,
    request: OfflineMaintenanceAuthorizationRequest,
    obligations: Obligations,
    capability_id: CapabilityId,
    capability_revision: NonZeroU64,
    principal_id: ActorId,
    actor_kind: ActorKind,
}

impl AuthorizedOfflineMaintenance {
    #[allow(clippy::too_many_arguments)]
    pub(crate) const fn new(
        database_id: DatabaseId,
        environment: Environment,
        request: OfflineMaintenanceAuthorizationRequest,
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

    /// Returns the exact database boundary checked by this decision.
    #[must_use]
    pub const fn database_id(&self) -> DatabaseId {
        self.database_id
    }

    /// Borrows the exact environment checked by this decision.
    #[must_use]
    pub const fn environment(&self) -> &Environment {
        &self.environment
    }

    /// Borrows the exact action request checked by this decision.
    #[must_use]
    pub const fn request(&self) -> &OfflineMaintenanceAuthorizationRequest {
        &self.request
    }

    /// Returns the exact process-local action checked by this decision.
    #[must_use]
    pub const fn operation(&self) -> OfflineMaintenancePolicyOperation {
        self.request.operation()
    }

    /// Borrows the ordinary canonical obligations produced by policy.
    #[must_use]
    pub const fn obligations(&self) -> &Obligations {
        &self.obligations
    }

    /// Returns the current capability identity checked by this decision.
    #[must_use]
    pub const fn authorizing_capability_id(&self) -> CapabilityId {
        self.capability_id
    }

    /// Returns the current capability revision checked by this decision.
    #[must_use]
    pub const fn authorizing_capability_revision(&self) -> NonZeroU64 {
        self.capability_revision
    }

    /// Borrows the admitted principal checked by this decision.
    #[must_use]
    pub const fn principal_id(&self) -> &ActorId {
        &self.principal_id
    }

    /// Returns the admitted actor classification checked by this decision.
    #[must_use]
    pub const fn actor_kind(&self) -> ActorKind {
        self.actor_kind
    }
}

impl fmt::Debug for AuthorizedOfflineMaintenance {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("AuthorizedOfflineMaintenance([REDACTED])")
    }
}

/// Deny-by-default result of one offline-maintenance policy safe point.
#[derive(Eq, PartialEq)]
pub enum OfflineMaintenanceDecision {
    /// Current policy allowed this exact maintenance action.
    Allow(Box<AuthorizedOfflineMaintenance>),
    /// Current policy denied the action with a closed internal code.
    Deny(PolicyCode),
}

impl fmt::Debug for OfflineMaintenanceDecision {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Allow(_) => formatter.write_str("OfflineMaintenanceDecision::Allow([REDACTED])"),
            Self::Deny(code) => formatter
                .debug_tuple("OfflineMaintenanceDecision::Deny")
                .field(code)
                .finish(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn operation_id() -> OfflineMaintenanceOperationId {
        OfflineMaintenanceOperationId::from_unix_milliseconds_and_random(1, [2; 10])
            .expect("valid UUIDv7")
    }

    fn input_hash() -> OfflineMaintenanceInputHash {
        OfflineMaintenanceInputHash::from_bytes([3; 32])
    }

    #[test]
    fn request_registry_is_closed_and_redacted() {
        let cases = [
            (
                OfflineMaintenanceAuthorizationRequest::retire_backup(operation_id(), input_hash()),
                OfflineMaintenancePolicyOperation::Start(
                    OfflineMaintenanceOperationKind::RetireBackup,
                ),
                Some(input_hash()),
            ),
            (
                OfflineMaintenanceAuthorizationRequest::create_backup(operation_id(), input_hash()),
                OfflineMaintenancePolicyOperation::Start(
                    OfflineMaintenanceOperationKind::CreateBackup,
                ),
                Some(input_hash()),
            ),
            (
                OfflineMaintenanceAuthorizationRequest::restore_backup(
                    operation_id(),
                    input_hash(),
                ),
                OfflineMaintenancePolicyOperation::Start(
                    OfflineMaintenanceOperationKind::RestoreBackup,
                ),
                Some(input_hash()),
            ),
            (
                OfflineMaintenanceAuthorizationRequest::get_operation(operation_id()),
                OfflineMaintenancePolicyOperation::GetOperation,
                None,
            ),
        ];

        for (request, expected, expected_hash) in cases {
            assert_eq!(request.operation_id(), operation_id());
            assert_eq!(request.operation(), expected);
            assert_eq!(request.input_hash(), expected_hash);
            assert_eq!(
                format!("{request:?}"),
                "OfflineMaintenanceAuthorizationRequest([REDACTED])"
            );
        }
    }
}
