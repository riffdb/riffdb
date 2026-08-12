//! Closed, current-capability authorization types for symbolic application export.

use std::fmt;
use std::num::{NonZeroU16, NonZeroU64};

use riffdb_types::{
    ActorId, ActorKind, ApplicationExportAuthorityV1, ApplicationExportOperationId,
    ApplicationExportSelectionV1, CapabilityId, DatabaseId, EntityFieldVisibilityV1, Environment,
    PartitionScopeV1,
};

use crate::{AuthorizedRowPolicyAuthority, Obligations, PolicyCode};

/// Closed process-local action checked at one export authorization safe point.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ApplicationExportPolicyOperationV1 {
    /// Capture an immutable source snapshot and create a durable checkpoint.
    Start,
    /// Release one bounded page.
    Page,
    /// Observe current protected progress or a terminal receipt.
    Status,
    /// Close one nonterminal operation.
    Cancel,
}

/// Exact operation, selection, and action supplied to current policy.
#[derive(Clone, Eq, PartialEq)]
pub struct ApplicationExportAuthorizationRequestV1 {
    operation_id: ApplicationExportOperationId,
    selection: ApplicationExportSelectionV1,
    operation: ApplicationExportPolicyOperationV1,
}

impl ApplicationExportAuthorizationRequestV1 {
    /// Constructs one exact safe-point request from service-owned operation state.
    #[must_use]
    pub const fn new(
        operation_id: ApplicationExportOperationId,
        selection: ApplicationExportSelectionV1,
        operation: ApplicationExportPolicyOperationV1,
    ) -> Self {
        Self {
            operation_id,
            selection,
            operation,
        }
    }

    /// Caller-stable export identity.
    #[must_use]
    pub const fn operation_id(&self) -> ApplicationExportOperationId {
        self.operation_id
    }

    /// Exact lineage, scope, and record classes checked by policy.
    #[must_use]
    pub const fn selection(&self) -> &ApplicationExportSelectionV1 {
        &self.selection
    }

    /// Current safe-point action.
    #[must_use]
    pub const fn operation(&self) -> ApplicationExportPolicyOperationV1 {
        self.operation
    }
}

impl fmt::Debug for ApplicationExportAuthorizationRequestV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ApplicationExportAuthorizationRequestV1([REDACTED])")
    }
}

/// Move-only proof that one exact export safe point used current V5 authority.
#[derive(Eq, PartialEq)]
pub struct AuthorizedApplicationExportV1 {
    database_id: DatabaseId,
    environment: Environment,
    request: ApplicationExportAuthorizationRequestV1,
    obligations: Obligations,
    authority: ApplicationExportAuthorityV1,
    principal_id: ActorId,
    actor_kind: ActorKind,
    partition_scope: PartitionScopeV1,
    max_scan_rows: NonZeroU16,
    field_visibility: Vec<EntityFieldVisibilityV1>,
    row_policy_authority: Option<AuthorizedRowPolicyAuthority>,
}

impl AuthorizedApplicationExportV1 {
    #[allow(clippy::too_many_arguments)]
    pub(crate) const fn new(
        database_id: DatabaseId,
        environment: Environment,
        request: ApplicationExportAuthorizationRequestV1,
        obligations: Obligations,
        capability_id: CapabilityId,
        capability_revision: NonZeroU64,
        principal_id: ActorId,
        actor_kind: ActorKind,
        partition_scope: PartitionScopeV1,
        max_scan_rows: NonZeroU16,
        field_visibility: Vec<EntityFieldVisibilityV1>,
        row_policy_authority: Option<AuthorizedRowPolicyAuthority>,
    ) -> Self {
        Self {
            database_id,
            environment,
            request,
            obligations,
            authority: ApplicationExportAuthorityV1::new(capability_id, capability_revision),
            principal_id,
            actor_kind,
            partition_scope,
            max_scan_rows,
            field_visibility,
            row_policy_authority,
        }
    }

    /// Exact database boundary checked by current policy.
    #[must_use]
    pub const fn database_id(&self) -> DatabaseId {
        self.database_id
    }

    /// Exact configured environment checked by current policy.
    #[must_use]
    pub const fn environment(&self) -> &Environment {
        &self.environment
    }

    /// Exact safe-point request checked by current policy.
    #[must_use]
    pub const fn request(&self) -> &ApplicationExportAuthorizationRequestV1 {
        &self.request
    }

    /// Ordinary canonical obligations for this release point.
    #[must_use]
    pub const fn obligations(&self) -> &Obligations {
        &self.obligations
    }

    /// Current capability identity and revision that receipts must bind.
    #[must_use]
    pub const fn authority(&self) -> ApplicationExportAuthorityV1 {
        self.authority
    }

    /// Admitted principal identity.
    #[must_use]
    pub const fn principal_id(&self) -> &ActorId {
        &self.principal_id
    }

    /// Admitted actor classification.
    #[must_use]
    pub const fn actor_kind(&self) -> ActorKind {
        self.actor_kind
    }

    /// Current bounded partition filter. Whole-application proofs carry `All`.
    #[doc(hidden)]
    #[must_use]
    pub const fn internal_partition_scope(&self) -> &PartitionScopeV1 {
        &self.partition_scope
    }

    /// Current per-page scan ceiling.
    #[doc(hidden)]
    #[must_use]
    pub const fn internal_max_scan_rows(&self) -> NonZeroU16 {
        self.max_scan_rows
    }

    /// Current field visibility applied before serialization.
    #[doc(hidden)]
    #[must_use]
    pub fn internal_field_visibility(&self) -> &[EntityFieldVisibilityV1] {
        &self.field_visibility
    }

    /// Current compiler-owned row-policy authority, required for principal scope.
    #[doc(hidden)]
    #[must_use]
    pub const fn internal_row_policy_authority(&self) -> Option<&AuthorizedRowPolicyAuthority> {
        self.row_policy_authority.as_ref()
    }
}

impl fmt::Debug for AuthorizedApplicationExportV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("AuthorizedApplicationExportV1([REDACTED])")
    }
}

/// Deny-by-default result of one transaction-current export safe point.
#[derive(Eq, PartialEq)]
pub enum ApplicationExportDecisionV1 {
    /// Current V5 authority allows the exact request.
    Allow(Box<AuthorizedApplicationExportV1>),
    /// Current authority denies the request with one closed policy code.
    Deny(PolicyCode),
}

impl fmt::Debug for ApplicationExportDecisionV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Allow(_) => formatter.write_str("ApplicationExportDecisionV1::Allow([REDACTED])"),
            Self::Deny(code) => formatter
                .debug_tuple("ApplicationExportDecisionV1::Deny")
                .field(code)
                .finish(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use riffdb_types::{CapabilityApplicationExportScopeV1, ContractLineage};

    #[test]
    fn request_is_closed_and_redacted() {
        let operation_id =
            ApplicationExportOperationId::from_unix_milliseconds_and_random(1, [4; 10])
                .expect("operation");
        let selection = ApplicationExportSelectionV1::new(
            ContractLineage::new("TicketDesk").expect("lineage"),
            CapabilityApplicationExportScopeV1::WholeApplication,
            true,
            false,
            false,
            false,
        )
        .expect("selection");
        let request = ApplicationExportAuthorizationRequestV1::new(
            operation_id,
            selection,
            ApplicationExportPolicyOperationV1::Page,
        );
        assert_eq!(request.operation_id(), operation_id);
        assert_eq!(
            request.operation(),
            ApplicationExportPolicyOperationV1::Page
        );
        assert_eq!(
            format!("{request:?}"),
            "ApplicationExportAuthorizationRequestV1([REDACTED])"
        );
    }
}
