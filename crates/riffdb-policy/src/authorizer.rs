//! Fresh current-capability authorization over closed operation facts.

use std::{error::Error, fmt};

use riffdb_auth::{
    AuthenticatedPrincipal, CurrentCapability, CurrentCapabilityActivity, CurrentCapabilityResolver,
};
use riffdb_types::{
    ActorId, ActorKind, Audience, CapabilityGrantV1, CapabilityId, DatabaseId, Environment,
    PartitionScopeV1, TenantScope, Timestamp,
};

use crate::decision::{PermissionCheck, check_permission, derive_field_mask};
use crate::operation::PartitionRequirement;
use crate::{
    AuthorizationClock, AuthorizationDefect, AuthorizationTelemetry, AuthorizationTelemetryEvent,
    AuthorizedOperation, Decision, Obligations, OperationRequest, PolicyCode,
};

/// A redaction-safe internal failure before policy could decide.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AuthorizationError {
    /// Current capability state could not be reloaded safely.
    CurrentCapabilityUnavailable,
    /// Fresh authorization time could not be sampled safely.
    ClockUnavailable,
}

impl fmt::Display for AuthorizationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::CurrentCapabilityUnavailable => "current authorization state is unavailable",
            Self::ClockUnavailable => "current authorization time is unavailable",
        })
    }
}

impl Error for AuthorizationError {}

/// Non-caching authorizer for one configured database and environment.
pub struct CurrentAuthorizer<'a, R: ?Sized, C: ?Sized, T: ?Sized> {
    resolver: &'a R,
    clock: &'a C,
    telemetry: &'a T,
    expected_database_id: DatabaseId,
    expected_environment: Environment,
}

impl<'a, R: ?Sized, C: ?Sized, T: ?Sized> CurrentAuthorizer<'a, R, C, T> {
    /// Wires the current-state sources and trusted server boundary.
    #[must_use]
    pub const fn new(
        resolver: &'a R,
        clock: &'a C,
        telemetry: &'a T,
        expected_database_id: DatabaseId,
        expected_environment: Environment,
    ) -> Self {
        Self {
            resolver,
            clock,
            telemetry,
            expected_database_id,
            expected_environment,
        }
    }
}

impl<R, C, T> CurrentAuthorizer<'_, R, C, T>
where
    R: CurrentCapabilityResolver + ?Sized,
    C: AuthorizationClock + ?Sized,
    T: AuthorizationTelemetry + ?Sized,
{
    /// Reloads current state, samples fresh time once, and evaluates one request.
    pub fn authorize(
        &self,
        principal: &AuthenticatedPrincipal,
        request: OperationRequest,
    ) -> Result<Decision, AuthorizationError> {
        let current = self.resolver.resolve_current(principal).map_err(|_| {
            self.telemetry.record(AuthorizationTelemetryEvent::Defect(
                AuthorizationDefect::CurrentCapabilityUnavailable,
            ));
            AuthorizationError::CurrentCapabilityUnavailable
        })?;
        let now = self.clock.now().map_err(|_| {
            self.telemetry.record(AuthorizationTelemetryEvent::Defect(
                AuthorizationDefect::ClockUnavailable,
            ));
            AuthorizationError::ClockUnavailable
        })?;

        let principal_facts = PrincipalFacts::from(principal);
        let current_facts = CurrentFacts::from(&current);
        match evaluate(
            &principal_facts,
            &current_facts,
            self.expected_database_id,
            &self.expected_environment,
            now,
            &request,
        ) {
            Ok(obligations) => {
                let proof = if request.permission_requirement().is_none() {
                    AuthorizedOperation::new_discovery(request, obligations, current_facts.grant)
                } else {
                    AuthorizedOperation::new(request, obligations)
                };
                Ok(Decision::Allow(Box::new(proof)))
            }
            Err(code) => {
                self.telemetry
                    .record(AuthorizationTelemetryEvent::Denied(code));
                Ok(Decision::Deny(code))
            }
        }
    }
}

#[derive(Clone)]
struct PrincipalFacts {
    capability_id: CapabilityId,
    capability_revision: std::num::NonZeroU64,
    principal_id: ActorId,
    actor_kind: ActorKind,
    audience: Audience,
    tenant_scope: TenantScope,
}

impl From<&AuthenticatedPrincipal> for PrincipalFacts {
    fn from(principal: &AuthenticatedPrincipal) -> Self {
        Self {
            capability_id: principal.capability_id(),
            capability_revision: principal.capability_revision(),
            principal_id: principal.principal_id().clone(),
            actor_kind: principal.actor_kind(),
            audience: principal.audience().clone(),
            tenant_scope: principal.tenant_scope().clone(),
        }
    }
}

#[derive(Clone)]
struct CurrentFacts {
    capability_id: CapabilityId,
    revision: std::num::NonZeroU64,
    activity: CurrentCapabilityActivity,
    database_id: DatabaseId,
    environment: Environment,
    principal_id: ActorId,
    actor_kind: ActorKind,
    audiences: Vec<Audience>,
    issued_at: Timestamp,
    expires_at: Timestamp,
    grant: CapabilityGrantV1,
}

impl From<&CurrentCapability> for CurrentFacts {
    fn from(current: &CurrentCapability) -> Self {
        Self {
            capability_id: current.capability_id(),
            revision: current.revision(),
            activity: current.activity(),
            database_id: current.database_id(),
            environment: current.environment().clone(),
            principal_id: current.principal_id().clone(),
            actor_kind: current.actor_kind(),
            audiences: current.audiences().to_vec(),
            issued_at: current.issued_at(),
            expires_at: current.expires_at(),
            grant: current.grant().clone(),
        }
    }
}

fn evaluate(
    principal: &PrincipalFacts,
    current: &CurrentFacts,
    expected_database_id: DatabaseId,
    expected_environment: &Environment,
    now: Timestamp,
    request: &OperationRequest,
) -> Result<Obligations, PolicyCode> {
    validate_current(
        principal,
        current,
        expected_database_id,
        expected_environment,
        now,
    )?;

    if let Some(requirement) = request.permission_requirement() {
        match check_permission(&current.grant, &requirement) {
            PermissionCheck::Missing => return Err(PolicyCode::MissingPermission),
            PermissionCheck::ApprovalRequired => return Err(PolicyCode::ApprovalRequired),
            PermissionCheck::Allowed => {}
        }
    }

    let effective_tenant_scope =
        authorize_tenant(current.grant.tenant_scope(), request.tenant_requirement())?;
    if let Some(owner) = request.outcome_owner_requirement() {
        if owner.principal_id != &principal.principal_id {
            return Err(PolicyCode::MissingPermission);
        }
        if owner.tenant_scope != &effective_tenant_scope {
            return Err(PolicyCode::TenantScopeMismatch);
        }
    }
    let partition_constraint = match request.partition_requirement() {
        PartitionRequirement::None => None,
        PartitionRequirement::Exact(required) => match current.grant.partition_scope() {
            PartitionScopeV1::All => Some(crate::PartitionConstraint::Exact(required.clone())),
            PartitionScopeV1::Explicit(entries) if entries.contains(required) => {
                Some(crate::PartitionConstraint::Exact(required.clone()))
            }
            PartitionScopeV1::Explicit(_) => return Err(PolicyCode::PartitionScopeMismatch),
        },
        PartitionRequirement::Filter => Some(crate::PartitionConstraint::Filter(
            current.grant.partition_scope().clone(),
        )),
        PartitionRequirement::AllOnly => match current.grant.partition_scope() {
            PartitionScopeV1::All => {
                Some(crate::PartitionConstraint::Filter(PartitionScopeV1::All))
            }
            PartitionScopeV1::Explicit(_) => return Err(PolicyCode::PartitionScopeMismatch),
        },
    };
    let field_mask = request
        .field_requirement()
        .map(|requirement| derive_field_mask(&current.grant, requirement));
    let row_limit = request
        .requested_rows()
        .map(|requested| requested.min(current.grant.max_scan_rows()));

    if request.needs_delegation_facts() {
        return Err(PolicyCode::DelegationExceedsAuthority);
    }

    Ok(Obligations::new(
        effective_tenant_scope,
        partition_constraint,
        field_mask,
        row_limit,
        None,
        request.audit_obligation(),
        request.output_classification(),
    ))
}

fn validate_current(
    principal: &PrincipalFacts,
    current: &CurrentFacts,
    expected_database_id: DatabaseId,
    expected_environment: &Environment,
    now: Timestamp,
) -> Result<(), PolicyCode> {
    let valid = current.activity == CurrentCapabilityActivity::Active
        && current.capability_id == principal.capability_id
        && current.revision == principal.capability_revision
        && current.database_id == expected_database_id
        && &current.environment == expected_environment
        && current.principal_id == principal.principal_id
        && current.actor_kind == principal.actor_kind
        && current.audiences.binary_search(&principal.audience).is_ok()
        && current.grant.tenant_scope() == &principal.tenant_scope
        && current.issued_at <= now
        && now < current.expires_at;
    if valid {
        Ok(())
    } else {
        Err(PolicyCode::InactiveOrStaleCapability)
    }
}

fn authorize_tenant(
    grant: &TenantScope,
    required: Option<&crate::OperationTenantScope>,
) -> Result<TenantScope, PolicyCode> {
    let Some(_required) = required else {
        return Ok(grant.clone());
    };
    if matches!(grant, TenantScope::Global) {
        Ok(TenantScope::Global)
    } else {
        Err(PolicyCode::TenantScopeMismatch)
    }
}

#[cfg(test)]
mod tests {
    use std::num::{NonZeroU16, NonZeroU64};

    use riffdb_types::{
        AggregateTypeId, CapabilityPermissionKindV1, CapabilityPermissionV1,
        CapabilityPermissionsV1, CommandId, ContractLineage, EntityFieldVisibilityV1, EntityTypeId,
        FieldId, IndexId, PartitionKeyBuilder, ProjectionId, ProjectionIdentity,
        ProjectionPlanHash, ScopedPartitionV1, TenantId,
    };

    use super::*;
    use crate::{CommandExecutionClass, OperationTenantScope, PartitionConstraint};

    fn timestamp(seconds: i64) -> Timestamp {
        Timestamp::new(seconds, 0).expect("valid timestamp")
    }

    fn database_id() -> DatabaseId {
        DatabaseId::from_unix_milliseconds_and_random(1, [1; 10]).expect("valid UUIDv7")
    }

    fn capability_id() -> CapabilityId {
        CapabilityId::from_unix_milliseconds_and_random(2, [2; 10]).expect("valid UUIDv7")
    }

    fn lineage() -> ContractLineage {
        ContractLineage::new("example.contract").expect("valid lineage")
    }

    fn partition() -> riffdb_types::PartitionKey {
        let mut builder = PartitionKeyBuilder::new(AggregateTypeId::first());
        builder.push_u64(7).expect("bounded component");
        builder.finish().expect("valid partition")
    }

    fn explicit_partition() -> PartitionScopeV1 {
        PartitionScopeV1::explicit(vec![ScopedPartitionV1::new(lineage(), partition())])
            .expect("explicit scope")
    }

    fn grant(
        tenant_scope: TenantScope,
        partition_scope: PartitionScopeV1,
        permissions: Vec<CapabilityPermissionV1>,
        field_visibility: Vec<EntityFieldVisibilityV1>,
        max_rows: u16,
        approval_required: Vec<CapabilityPermissionKindV1>,
    ) -> CapabilityGrantV1 {
        CapabilityGrantV1::new(
            tenant_scope,
            partition_scope,
            CapabilityPermissionsV1::new(permissions).expect("valid permissions"),
            field_visibility,
            NonZeroU16::new(max_rows).expect("nonzero rows"),
            approval_required,
        )
        .expect("valid grant")
    }

    fn facts(grant: CapabilityGrantV1) -> (PrincipalFacts, CurrentFacts, Environment) {
        let environment = Environment::new("dev").expect("valid environment");
        let audience = Audience::new("grpc").expect("valid audience");
        let principal_id = ActorId::new("principal-1").expect("valid actor");
        let revision = NonZeroU64::new(1).expect("nonzero revision");
        let principal = PrincipalFacts {
            capability_id: capability_id(),
            capability_revision: revision,
            principal_id: principal_id.clone(),
            actor_kind: ActorKind::Service,
            audience: audience.clone(),
            tenant_scope: grant.tenant_scope().clone(),
        };
        let current = CurrentFacts {
            capability_id: principal.capability_id,
            revision,
            activity: CurrentCapabilityActivity::Active,
            database_id: database_id(),
            environment: environment.clone(),
            principal_id,
            actor_kind: ActorKind::Service,
            audiences: vec![audience],
            issued_at: timestamp(10),
            expires_at: timestamp(20),
            grant,
        };
        (principal, current, environment)
    }

    #[test]
    fn grammar_v1_command_requires_global_tenant_and_exact_partition() {
        let permission = CapabilityPermissionV1::InvokeCommand(lineage(), CommandId::first());
        let request = OperationRequest::execute_command(
            lineage(),
            riffdb_types::ContractVersion::new(1).expect("nonzero version"),
            CommandId::first(),
            CommandExecutionClass::Mutation,
            partition(),
        );
        let tenant = TenantId::new("tenant-a").expect("valid tenant");
        let scoped = ScopedPartitionV1::new(lineage(), partition());
        let scan_grant = grant(
            TenantScope::Tenant(tenant),
            PartitionScopeV1::explicit(vec![scoped]).expect("explicit scope"),
            vec![permission],
            Vec::new(),
            50,
            Vec::new(),
        );
        let (principal, current, environment) = facts(scan_grant);
        assert_eq!(
            evaluate(
                &principal,
                &current,
                current.database_id,
                &environment,
                timestamp(15),
                &request,
            ),
            Err(PolicyCode::TenantScopeMismatch)
        );
    }

    #[test]
    fn entity_fields_intersect_and_row_limit_lowers() {
        let visibility =
            EntityFieldVisibilityV1::new(lineage(), EntityTypeId::first(), vec![FieldId::first()])
                .expect("valid visibility");
        let permission = CapabilityPermissionV1::ReadEntity(lineage(), EntityTypeId::first());
        let grant = grant(
            TenantScope::Global,
            PartitionScopeV1::All,
            vec![permission],
            vec![visibility],
            5,
            Vec::new(),
        );
        let request = OperationRequest::get_entity(
            lineage(),
            riffdb_types::ContractVersion::new(1).expect("nonzero version"),
            EntityTypeId::first(),
            OperationTenantScope::global_only(),
            partition(),
            vec![FieldId::first(), FieldId::new(2).expect("nonzero field")],
        )
        .expect("valid request");
        let (principal, current, environment) = facts(grant);
        let obligations = evaluate(
            &principal,
            &current,
            current.database_id,
            &environment,
            timestamp(15),
            &request,
        )
        .expect("allowed");
        assert_eq!(
            obligations.field_mask().expect("field mask").fields(),
            &[FieldId::first()]
        );
        assert_eq!(
            obligations.partition_constraint(),
            Some(&PartitionConstraint::Exact(ScopedPartitionV1::new(
                lineage(),
                partition(),
            )))
        );
        assert_eq!(obligations.audit_class(), None);
    }

    #[test]
    fn approval_and_lifecycle_changes_fail_closed() {
        let permission =
            CapabilityPermissionV1::unparameterized(CapabilityPermissionKindV1::ReadHealth)
                .expect("valid permission");
        let grant = grant(
            TenantScope::Global,
            PartitionScopeV1::All,
            vec![permission],
            Vec::new(),
            5,
            vec![CapabilityPermissionKindV1::ReadHealth],
        );
        let (principal, mut current, environment) = facts(grant);
        assert_eq!(
            evaluate(
                &principal,
                &current,
                current.database_id,
                &environment,
                timestamp(15),
                &OperationRequest::get_health(),
            ),
            Err(PolicyCode::ApprovalRequired)
        );
        current.activity = CurrentCapabilityActivity::Revoked;
        assert_eq!(
            evaluate(
                &principal,
                &current,
                current.database_id,
                &environment,
                timestamp(15),
                &OperationRequest::get_health(),
            ),
            Err(PolicyCode::InactiveOrStaleCapability)
        );
    }

    #[test]
    fn every_current_binding_and_half_open_time_window_fail_closed() {
        let permission =
            CapabilityPermissionV1::unparameterized(CapabilityPermissionKindV1::ReadHealth)
                .expect("valid permission");
        let grant = grant(
            TenantScope::Global,
            PartitionScopeV1::All,
            vec![permission],
            Vec::new(),
            5,
            Vec::new(),
        );
        let (principal, current, environment) = facts(grant);
        let expected_database = current.database_id;
        let request = OperationRequest::get_health();
        assert!(
            evaluate(
                &principal,
                &current,
                expected_database,
                &environment,
                timestamp(10),
                &request,
            )
            .is_ok()
        );
        for now in [timestamp(9), timestamp(20)] {
            assert_eq!(
                evaluate(
                    &principal,
                    &current,
                    expected_database,
                    &environment,
                    now,
                    &request,
                ),
                Err(PolicyCode::InactiveOrStaleCapability)
            );
        }

        let mut changed_current = Vec::new();
        let mut value = current.clone();
        value.capability_id =
            CapabilityId::from_unix_milliseconds_and_random(3, [3; 10]).expect("valid UUIDv7");
        changed_current.push(value);
        let mut value = current.clone();
        value.revision = NonZeroU64::new(2).expect("nonzero revision");
        changed_current.push(value);
        let mut value = current.clone();
        value.environment = Environment::new("other").expect("valid environment");
        changed_current.push(value);
        let mut value = current.clone();
        value.principal_id = ActorId::new("other-principal").expect("valid actor");
        changed_current.push(value);
        let mut value = current.clone();
        value.actor_kind = ActorKind::Human;
        changed_current.push(value);
        let mut value = current.clone();
        value.audiences.clear();
        changed_current.push(value);
        for changed in changed_current {
            assert_eq!(
                evaluate(
                    &principal,
                    &changed,
                    expected_database,
                    &environment,
                    timestamp(15),
                    &request,
                ),
                Err(PolicyCode::InactiveOrStaleCapability)
            );
        }

        let mut changed_principal = principal.clone();
        changed_principal.tenant_scope =
            TenantScope::Tenant(TenantId::new("tenant-a").expect("valid tenant"));
        assert_eq!(
            evaluate(
                &changed_principal,
                &current,
                expected_database,
                &environment,
                timestamp(15),
                &request,
            ),
            Err(PolicyCode::InactiveOrStaleCapability)
        );
    }

    #[test]
    fn scan_row_limit_lowers_and_projection_requires_all_partitions() {
        let scan_permission = CapabilityPermissionV1::ScanIndex(lineage(), IndexId::first());
        let scan_grant = grant(
            TenantScope::Global,
            PartitionScopeV1::All,
            vec![scan_permission],
            Vec::new(),
            5,
            Vec::new(),
        );
        let request = OperationRequest::scan_index(
            lineage(),
            riffdb_types::ContractVersion::new(1).expect("nonzero version"),
            IndexId::first(),
            EntityTypeId::first(),
            OperationTenantScope::global_only(),
            Vec::new(),
            NonZeroU16::new(10).expect("nonzero rows"),
        )
        .expect("valid scan");
        let (principal, current, environment) = facts(scan_grant);
        let obligations = evaluate(
            &principal,
            &current,
            current.database_id,
            &environment,
            timestamp(15),
            &request,
        )
        .expect("allowed scan");
        assert_eq!(obligations.row_limit().map(NonZeroU16::get), Some(5));
        assert_eq!(
            obligations.partition_constraint(),
            Some(&PartitionConstraint::Filter(PartitionScopeV1::All))
        );

        let identity = ProjectionIdentity::new(
            lineage(),
            ProjectionId::first(),
            ProjectionPlanHash::from_bytes([7; 32]),
        );
        let projection_permission =
            CapabilityPermissionV1::QueryProjection(lineage(), ProjectionId::first());
        let projection_grant = grant(
            TenantScope::Global,
            explicit_partition(),
            vec![projection_permission],
            Vec::new(),
            5,
            Vec::new(),
        );
        let projection = OperationRequest::query_projection(
            riffdb_types::ContractVersion::new(1).expect("nonzero version"),
            identity,
            Vec::new(),
            NonZeroU16::new(10).expect("nonzero rows"),
        )
        .expect("valid projection selector");
        let (principal, current, environment) = facts(projection_grant);
        assert_eq!(
            evaluate(
                &principal,
                &current,
                current.database_id,
                &environment,
                timestamp(15),
                &projection,
            ),
            Err(PolicyCode::PartitionScopeMismatch)
        );
    }

    #[test]
    fn outcome_resolution_requires_the_recorded_owner_and_tenant() {
        let permission = CapabilityPermissionV1::InvokeCommand(lineage(), CommandId::first());
        let owner_grant = grant(
            TenantScope::Global,
            PartitionScopeV1::All,
            vec![permission],
            Vec::new(),
            5,
            Vec::new(),
        );
        let (principal, current, environment) = facts(owner_grant);
        let request = |owner_principal_id, owner_tenant_scope| {
            OperationRequest::resolve_command_outcome(
                lineage(),
                riffdb_types::ContractVersion::new(1).expect("nonzero version"),
                CommandId::first(),
                owner_principal_id,
                owner_tenant_scope,
                partition(),
            )
        };
        assert!(
            evaluate(
                &principal,
                &current,
                current.database_id,
                &environment,
                timestamp(15),
                &request(principal.principal_id.clone(), TenantScope::Global),
            )
            .is_ok()
        );
        assert_eq!(
            evaluate(
                &principal,
                &current,
                current.database_id,
                &environment,
                timestamp(15),
                &request(
                    ActorId::new("other-principal").expect("valid actor"),
                    TenantScope::Global,
                ),
            ),
            Err(PolicyCode::MissingPermission)
        );
        assert_eq!(
            evaluate(
                &principal,
                &current,
                current.database_id,
                &environment,
                timestamp(15),
                &request(
                    principal.principal_id.clone(),
                    TenantScope::Tenant(TenantId::new("tenant-a").expect("valid tenant")),
                ),
            ),
            Err(PolicyCode::TenantScopeMismatch)
        );
    }
}
