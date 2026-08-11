//! Fresh current-capability authorization over closed operation facts.

use std::{error::Error, fmt};

use riffdb_auth::{
    AuthenticatedPrincipal, CurrentCapability, CurrentCapabilityActivity, CurrentCapabilityResolver,
};
use riffdb_types::{
    ActorId, ActorKind, Audience, CapabilityGrantV1, CapabilityId, CapabilityPermissionKindV1,
    DatabaseId, Environment, PartitionScopeV1, ServiceOperationV1, TenantScope, Timestamp,
};

use crate::decision::{PermissionCheck, check_permission, derive_field_mask};
use crate::operation::PartitionRequirement;
use crate::{
    AuthorizationClock, AuthorizationDefect, AuthorizationTelemetry, AuthorizationTelemetryEvent,
    AuthorizedCapabilityMutationPreparation, AuthorizedContractMigration,
    AuthorizedOfflineMaintenance, AuthorizedOperation, AuthorizedRowPolicyAuthority,
    CapabilityActivity, CapabilityMutationRequest, CheckedCapabilityValidity,
    ContractMigrationAuthorizationRequest, ContractMigrationDecision, CurrentAuthorizationIdentity,
    Decision, Obligations, OfflineMaintenanceAuthorizationRequest, OfflineMaintenanceDecision,
    OperationRequest, OutputClassification, PolicyCode, TransactionCurrentCapabilityFacts,
    TrustedAudienceCatalog,
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
    trusted_audience_catalog: Option<&'a TrustedAudienceCatalog>,
}

impl<'a, R: ?Sized, C: ?Sized, T: ?Sized> CurrentAuthorizer<'a, R, C, T> {
    /// Wires the current-state sources and trusted server boundary.
    ///
    /// The base configuration has no audience catalog. Read authorization is
    /// unaffected, while capability creation fails closed with the existing
    /// redacted delegation denial until
    /// [`Self::with_trusted_audience_catalog`] is called. This deliberately
    /// avoids exposing server configuration state as a caller-visible defect.
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
            trusted_audience_catalog: None,
        }
    }

    /// Attaches the complete trusted audience configuration used for capability creation.
    #[must_use]
    pub const fn with_trusted_audience_catalog(
        mut self,
        trusted_audience_catalog: &'a TrustedAudienceCatalog,
    ) -> Self {
        self.trusted_audience_catalog = Some(trusted_audience_catalog);
        self
    }
}

impl<R, C, T> CurrentAuthorizer<'_, R, C, T>
where
    R: CurrentCapabilityResolver + ?Sized,
    C: AuthorizationClock + ?Sized,
    T: AuthorizationTelemetry + ?Sized,
{
    /// Reloads current state and evaluates one offline-maintenance safe point.
    ///
    /// Restore callers must invoke this independently against the healthy
    /// current database and the fully validated staged database. An earlier
    /// proof is neither cached nor accepted as input to this method.
    pub fn authorize_offline_maintenance(
        &self,
        principal: &AuthenticatedPrincipal,
        request: OfflineMaintenanceAuthorizationRequest,
    ) -> Result<OfflineMaintenanceDecision, AuthorizationError> {
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
        match evaluate_offline_maintenance(
            &principal_facts,
            &current_facts,
            self.expected_database_id,
            &self.expected_environment,
            now,
        ) {
            Ok(obligations) => Ok(OfflineMaintenanceDecision::Allow(Box::new(
                AuthorizedOfflineMaintenance::new(
                    self.expected_database_id,
                    self.expected_environment.clone(),
                    request,
                    obligations,
                    current_facts.capability_id,
                    current_facts.revision,
                    principal_facts.principal_id,
                    principal_facts.actor_kind,
                ),
            ))),
            Err(code) => {
                self.telemetry
                    .record(AuthorizationTelemetryEvent::Denied(code));
                Ok(OfflineMaintenanceDecision::Deny(code))
            }
        }
    }

    /// Reloads current state and evaluates one exact contract-migration safe point.
    pub fn authorize_contract_migration(
        &self,
        principal: &AuthenticatedPrincipal,
        request: ContractMigrationAuthorizationRequest,
    ) -> Result<ContractMigrationDecision, AuthorizationError> {
        let current = self.resolver.resolve_current(principal).map_err(|_| {
            self.telemetry.record(AuthorizationTelemetryEvent::Defect(
                AuthorizationDefect::CurrentCapabilityUnavailable,
            ));
            AuthorizationError::CurrentCapabilityUnavailable
        })?;
        let now = self
            .clock
            .now()
            .map_err(|_| AuthorizationError::ClockUnavailable)?;
        let principal_facts = PrincipalFacts::from(principal);
        let current_facts = CurrentFacts::from(&current);
        match evaluate_contract_migration(
            &principal_facts,
            &current_facts,
            self.expected_database_id,
            &self.expected_environment,
            now,
            request.lineage(),
        ) {
            Ok(obligations) => Ok(ContractMigrationDecision::Allow(Box::new(
                AuthorizedContractMigration::new(
                    self.expected_database_id,
                    self.expected_environment.clone(),
                    request,
                    obligations,
                    current_facts.capability_id,
                    current_facts.revision,
                    principal_facts.principal_id,
                    principal_facts.actor_kind,
                ),
            ))),
            Err(code) => {
                self.telemetry
                    .record(AuthorizationTelemetryEvent::Denied(code));
                Ok(ContractMigrationDecision::Deny(code))
            }
        }
    }

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
        let row_policy_authority = match (
            current.row_policy_principal_binding(),
            current.grant().internal_row_policy(),
        ) {
            (None, None) => None,
            (Some(Ok(principal)), Some(grant)) => {
                Some(AuthorizedRowPolicyAuthority::new(principal, grant.clone()))
            }
            (Some(Err(_)), Some(_)) | (None, Some(_)) | (Some(_), None) => {
                self.telemetry.record(AuthorizationTelemetryEvent::Defect(
                    AuthorizationDefect::CurrentCapabilityUnavailable,
                ));
                return Err(AuthorizationError::CurrentCapabilityUnavailable);
            }
        };

        let principal_facts = PrincipalFacts::from(principal);
        let current_facts = CurrentFacts::from(&current);
        if matches!(
            request.operation(),
            ServiceOperationV1::CreateCapability | ServiceOperationV1::RevokeCapability
        ) {
            let preparation = self.prepare_capability_mutation(
                &principal_facts,
                &current_facts,
                &current,
                now,
                request,
            );
            return match preparation {
                Ok(preparation) => Ok(Decision::PrepareCapabilityMutation(Box::new(preparation))),
                Err(code) => {
                    self.telemetry
                        .record(AuthorizationTelemetryEvent::Denied(code));
                    Ok(Decision::Deny(code))
                }
            };
        }
        match evaluate(
            &principal_facts,
            &current_facts,
            self.expected_database_id,
            &self.expected_environment,
            now,
            &request,
        ) {
            Ok(obligations) => {
                let identity = CurrentAuthorizationIdentity::new(
                    current_facts.capability_id,
                    current_facts.revision,
                    principal_facts.principal_id,
                    principal_facts.actor_kind,
                    // Retained so revision-checked reauthorization can apply the
                    // exact same time clause without reloading the record.
                    current_validity(&current_facts),
                );
                let mut proof = if request.permission_requirement().is_none()
                    || request.operation() == ServiceOperationV1::DescribeContract
                {
                    AuthorizedOperation::new_discovery(
                        self.expected_database_id,
                        self.expected_environment.clone(),
                        request,
                        obligations,
                        current_facts.grant,
                        identity,
                    )
                } else {
                    AuthorizedOperation::new(
                        self.expected_database_id,
                        self.expected_environment.clone(),
                        request,
                        obligations,
                        identity,
                    )
                };
                proof.bind_row_policy_authority(row_policy_authority);
                Ok(Decision::Allow(Box::new(proof)))
            }
            Err(code) => {
                self.telemetry
                    .record(AuthorizationTelemetryEvent::Denied(code));
                Ok(Decision::Deny(code))
            }
        }
    }

    fn prepare_capability_mutation(
        &self,
        principal: &PrincipalFacts,
        current: &CurrentFacts,
        resolved: &CurrentCapability,
        now: Timestamp,
        request: OperationRequest,
    ) -> Result<AuthorizedCapabilityMutationPreparation, PolicyCode> {
        validate_current(
            principal,
            current,
            self.expected_database_id,
            &self.expected_environment,
            now,
        )?;
        let mutation = request
            .into_capability_mutation()
            .ok_or(PolicyCode::DelegationExceedsAuthority)?;
        let transaction_current = transaction_current_facts(resolved)?;
        match mutation {
            CapabilityMutationRequest::Create(target) => {
                let catalog = self
                    .trusted_audience_catalog
                    .ok_or(PolicyCode::DelegationExceedsAuthority)?;
                AuthorizedCapabilityMutationPreparation::create(
                    &transaction_current,
                    &principal.principal_id,
                    principal.actor_kind,
                    &principal.audience,
                    &principal.tenant_scope,
                    catalog,
                    now,
                    target,
                )
            }
            CapabilityMutationRequest::Revoke { target, reason } => {
                AuthorizedCapabilityMutationPreparation::revoke(
                    &transaction_current,
                    &principal.principal_id,
                    principal.actor_kind,
                    &principal.audience,
                    &principal.tenant_scope,
                    target,
                    reason,
                )
            }
            CapabilityMutationRequest::RevokeAbsent { target, reason } => {
                AuthorizedCapabilityMutationPreparation::revoke_absent(
                    &transaction_current,
                    &principal.principal_id,
                    principal.actor_kind,
                    &principal.audience,
                    &principal.tenant_scope,
                    target,
                    reason,
                )
            }
        }
    }
}

fn evaluate_offline_maintenance(
    principal: &PrincipalFacts,
    current: &CurrentFacts,
    expected_database_id: DatabaseId,
    expected_environment: &Environment,
    now: Timestamp,
) -> Result<Obligations, PolicyCode> {
    validate_current(
        principal,
        current,
        expected_database_id,
        expected_environment,
        now,
    )?;
    if current.grant.tenant_scope() != &TenantScope::Global {
        return Err(PolicyCode::TenantScopeMismatch);
    }

    let requirement = crate::operation::PermissionRequirement::Kind(
        CapabilityPermissionKindV1::AdministerCapabilities,
    );
    match check_permission(&current.grant, &requirement) {
        PermissionCheck::Missing => return Err(PolicyCode::MissingPermission),
        PermissionCheck::ApprovalRequired => return Err(PolicyCode::ApprovalRequired),
        PermissionCheck::Allowed => {}
    }

    Ok(Obligations::new(
        TenantScope::Global,
        None,
        None,
        None,
        None,
        None,
        OutputClassification::AdministrativeRedactedData,
    ))
}

fn evaluate_contract_migration(
    principal: &PrincipalFacts,
    current: &CurrentFacts,
    expected_database_id: DatabaseId,
    expected_environment: &Environment,
    now: Timestamp,
    lineage: &riffdb_types::ContractLineage,
) -> Result<Obligations, PolicyCode> {
    validate_current(
        principal,
        current,
        expected_database_id,
        expected_environment,
        now,
    )?;
    if current.grant.tenant_scope() != &TenantScope::Global {
        return Err(PolicyCode::TenantScopeMismatch);
    }
    if !current.grant.permissions().as_slice().iter().any(|permission| {
        matches!(permission, riffdb_types::CapabilityPermissionV1::MigrateContract(candidate) if candidate == lineage)
    }) {
        return Err(PolicyCode::MissingPermission);
    }
    if current
        .grant
        .approval_required()
        .contains(&CapabilityPermissionKindV1::MigrateContract)
    {
        return Err(PolicyCode::ApprovalRequired);
    }
    Ok(Obligations::new(
        TenantScope::Global,
        None,
        None,
        None,
        None,
        None,
        OutputClassification::AdministrativeRedactedData,
    ))
}

fn transaction_current_facts(
    current: &CurrentCapability,
) -> Result<TransactionCurrentCapabilityFacts, PolicyCode> {
    let activity = match current.activity() {
        CurrentCapabilityActivity::Active => CapabilityActivity::Active,
        CurrentCapabilityActivity::Revoked => CapabilityActivity::Revoked,
    };
    TransactionCurrentCapabilityFacts::new(
        current.capability_id(),
        current.revision(),
        activity,
        current.database_id(),
        current.environment().clone(),
        current.principal_id().clone(),
        current.actor_kind(),
        current.audiences().to_vec(),
        current.issued_at(),
        current.expires_at(),
        current.grant().clone(),
    )
    .map_err(|_| PolicyCode::InactiveOrStaleCapability)
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

    // WP-572 staged rollout: V4 can be persisted and reloaded before any
    // protected surface is enabled. Each arm is removed only when that surface
    // consumes the transaction-current policy authority before disclosure or
    // at the final commit safe point.
    if current.grant.internal_row_policy().is_some()
        && matches!(
            request.operation(),
            ServiceOperationV1::ExecuteProjectedQuery | ServiceOperationV1::ConsumeEventStream
        )
    {
        return Err(PolicyCode::MissingPermission);
    }

    if let Some(requirement) = request.permission_requirement() {
        match check_permission(&current.grant, &requirement) {
            PermissionCheck::Missing => return Err(PolicyCode::MissingPermission),
            PermissionCheck::ApprovalRequired => return Err(PolicyCode::ApprovalRequired),
            PermissionCheck::Allowed => {}
        }
    }
    if let Some(target) = request.application_query_target() {
        let rows = u64::from(current.grant.max_scan_rows().get());
        let row_work = rows.saturating_mul(riffdb_types::MAX_APPLICATION_QUERY_STEPS);
        let projected_values =
            row_work.saturating_mul(riffdb_types::MAX_CAPABILITY_FIELD_VISIBILITY as u64);
        let budget = riffdb_types::QueryCostVectorV1::new(
            riffdb_types::MAX_APPLICATION_QUERY_STEPS,
            rows,
            row_work,
            row_work,
            row_work,
            projected_values,
            riffdb_types::MAX_APPLICATION_QUERY_RESULT_BYTES,
        )
        .expect("application-query budget constants are valid");
        if !budget.covers(target.cost())
            || !application_query_accesses_visible(&current.grant, target)
        {
            return Err(PolicyCode::MissingPermission);
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

fn application_query_accesses_visible(
    grant: &CapabilityGrantV1,
    target: &crate::ApplicationQueryTarget,
) -> bool {
    crate::decision::application_query_accesses_visible(grant, target.lineage(), target.accesses())
}

/// Projects the exact validity window carried by reloaded current facts.
fn current_validity(current: &CurrentFacts) -> CheckedCapabilityValidity {
    CheckedCapabilityValidity::new(current.issued_at, current.expires_at)
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
        // Single definition of the time clause; revision-checked
        // reauthorization applies the same predicate to the retained window.
        && current_validity(current).admits(now);
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
    let Some(required) = required else {
        return Ok(grant.clone());
    };
    if grant == required.tenant_scope() {
        Ok(grant.clone())
    } else {
        Err(PolicyCode::TenantScopeMismatch)
    }
}

#[cfg(test)]
mod tests {
    use std::num::{NonZeroU16, NonZeroU32, NonZeroU64};

    use riffdb_testkit::authorization::{
        AuthorizationFixture, AuthorizationFixtureConfig, AuthorizationFixtureTimes,
    };
    use riffdb_types::{
        AggregateTypeId, ApplicationRoleHash, CapabilityPermissionKindV1, CapabilityPermissionV1,
        CapabilityPermissionsV1, CapabilityPrincipalFactsV1, CapabilityRowPolicyBindingV1,
        CapabilityRowPolicyGrantV1, CapabilityRowPolicyOperationV1, CommandId, CommitSequence,
        ContractBundleHash, ContractLineage, ContractVersion, EntityFieldVisibilityV1,
        EntityTypeId, EventConsumerName, FieldId, IndexId, PartitionKeyBuilder, ProjectionId,
        ProjectionIdentity, ProjectionPlanHash, QueryModuleHash, QueryOperationName,
        QueryParameterHash, QueryPlanHash, ReactiveModuleHash, ReactiveOperationName, RequestId,
        RowPolicyName, ScopedPartitionV1, ServiceIngressKindV1, TenantId,
    };

    use super::*;
    use crate::{
        ApplicationQueryAccessRequirement, ApplicationQueryTarget, CommandExecutionClass,
        ContractMigrationPolicyOperation, EventConsumerOperationTarget, OperationTenantScope,
        PartitionConstraint,
    };

    struct FixedAuthorizationClock(Timestamp);

    impl AuthorizationClock for FixedAuthorizationClock {
        fn now(&self) -> Result<Timestamp, crate::AuthorizationClockError> {
            Ok(self.0)
        }
    }

    fn timestamp(seconds: i64) -> Timestamp {
        Timestamp::new(seconds, 0).expect("valid timestamp")
    }

    fn database_id() -> DatabaseId {
        DatabaseId::from_unix_milliseconds_and_random(1, [1; 10]).expect("valid UUIDv7")
    }

    fn capability_id() -> CapabilityId {
        CapabilityId::from_unix_milliseconds_and_random(2, [2; 10]).expect("valid UUIDv7")
    }

    fn maintenance_operation_id() -> riffdb_types::OfflineMaintenanceOperationId {
        riffdb_types::OfflineMaintenanceOperationId::from_unix_milliseconds_and_random(3, [3; 10])
            .expect("valid UUIDv7")
    }

    fn maintenance_input_hash() -> riffdb_types::OfflineMaintenanceInputHash {
        riffdb_types::OfflineMaintenanceInputHash::from_bytes([4; 32])
    }

    fn migration_operation_id() -> riffdb_types::ContractMigrationOperationId {
        riffdb_types::ContractMigrationOperationId::from_unix_milliseconds_and_random(4, [4; 10])
            .expect("valid UUIDv7")
    }

    fn migration_input_hash() -> riffdb_types::ContractMigrationInputHash {
        riffdb_types::ContractMigrationInputHash::from_bytes([5; 32])
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

    fn application_query_target() -> ApplicationQueryTarget {
        application_query_target_with_cost(
            riffdb_types::QueryCostVectorV1::new(1, 10, 10, 0, 10, 10, 1_024).expect("valid cost"),
        )
    }

    fn application_query_target_with_cost(
        cost: riffdb_types::QueryCostVectorV1,
    ) -> ApplicationQueryTarget {
        ApplicationQueryTarget::new(
            lineage(),
            ContractVersion::new(1).expect("nonzero version"),
            ContractBundleHash::from_bytes([6; 32]),
            QueryPlanHash::from_bytes([7; 32]),
            ServiceIngressKindV1::InProcessTestComparison,
            OperationTenantScope::global_only(),
            partition(),
            vec![
                ApplicationQueryAccessRequirement::new(
                    EntityTypeId::first(),
                    Some(IndexId::first()),
                    vec![FieldId::first()],
                    NonZeroU16::new(10).expect("nonzero"),
                )
                .expect("valid access"),
            ],
            cost,
        )
        .expect("valid target")
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
    fn exact_named_query_authority_is_not_kernel_or_ad_hoc_authority() {
        let module_hash = QueryModuleHash::from_bytes([8; 32]);
        let query_name = QueryOperationName::new("TicketPage").expect("valid name");
        let permission =
            CapabilityPermissionV1::ExecuteNamedQuery(lineage(), module_hash, query_name.clone());
        let (principal, current, environment) = facts(grant(
            TenantScope::Global,
            PartitionScopeV1::All,
            vec![permission],
            vec![
                EntityFieldVisibilityV1::new(
                    lineage(),
                    EntityTypeId::first(),
                    vec![FieldId::first()],
                )
                .expect("field visibility"),
            ],
            100,
            Vec::new(),
        ));
        let named = |module_hash, query_name| {
            OperationRequest::execute_named_query(
                lineage(),
                module_hash,
                query_name,
                application_query_target(),
            )
            .expect("matching query target")
        };
        assert!(
            evaluate(
                &principal,
                &current,
                current.database_id,
                &environment,
                timestamp(15),
                &named(module_hash, query_name.clone()),
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
                &named(QueryModuleHash::from_bytes([9; 32]), query_name.clone()),
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
                &named(
                    module_hash,
                    QueryOperationName::new("OtherPage").expect("valid name"),
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
                &OperationRequest::execute_ad_hoc_query(application_query_target()),
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
                &OperationRequest::get_entity(
                    lineage(),
                    ContractVersion::new(1).expect("nonzero"),
                    EntityTypeId::first(),
                    OperationTenantScope::global_only(),
                    partition(),
                    vec![FieldId::first()],
                )
                .expect("valid read"),
            ),
            Err(PolicyCode::MissingPermission)
        );

        let (kernel_principal, kernel_current, kernel_environment) = facts(grant(
            TenantScope::Global,
            PartitionScopeV1::All,
            vec![
                CapabilityPermissionV1::ReadEntity(lineage(), EntityTypeId::first()),
                CapabilityPermissionV1::ScanIndex(lineage(), IndexId::first()),
            ],
            vec![
                EntityFieldVisibilityV1::new(
                    lineage(),
                    EntityTypeId::first(),
                    vec![FieldId::first()],
                )
                .expect("field visibility"),
            ],
            100,
            Vec::new(),
        ));
        assert_eq!(
            evaluate(
                &kernel_principal,
                &kernel_current,
                kernel_current.database_id,
                &kernel_environment,
                timestamp(15),
                &named(module_hash, query_name),
            ),
            Err(PolicyCode::MissingPermission)
        );
    }

    #[test]
    fn persisted_row_policy_authority_reaches_query_and_command_consumers() {
        let role = ApplicationRoleHash::from_bytes([0x44; 32]);
        let reactive_module = ReactiveModuleHash::from_bytes([0x45; 32]);
        let reactive_operation = ReactiveOperationName::new("DocumentWatch").expect("operation");
        let base = grant(
            TenantScope::Global,
            PartitionScopeV1::All,
            vec![
                CapabilityPermissionV1::Unparameterized(
                    CapabilityPermissionKindV1::ExecuteAdHocQuery,
                ),
                CapabilityPermissionV1::InvokeCommand(lineage(), CommandId::first()),
                CapabilityPermissionV1::WatchNamedQuery(
                    lineage(),
                    reactive_module,
                    reactive_operation.clone(),
                ),
                CapabilityPermissionV1::ConsumeContextualSubscription(
                    lineage(),
                    reactive_module,
                    ReactiveOperationName::new("DocumentAgent").expect("operation"),
                ),
                CapabilityPermissionV1::ApplicationRoleIdentity(role),
            ],
            vec![
                EntityFieldVisibilityV1::new(
                    lineage(),
                    EntityTypeId::first(),
                    vec![FieldId::first()],
                )
                .expect("visibility"),
            ],
            100,
            Vec::new(),
        );
        let protected = base
            .with_row_policy(
                CapabilityRowPolicyGrantV1::new(
                    role,
                    CapabilityPrincipalFactsV1::empty(),
                    vec![
                        CapabilityRowPolicyBindingV1::new(
                            lineage(),
                            RowPolicyName::new("DocumentAccess").expect("policy"),
                            EntityTypeId::first(),
                            vec![
                                CapabilityRowPolicyOperationV1::Read,
                                CapabilityRowPolicyOperationV1::Create,
                            ],
                        )
                        .expect("binding"),
                    ],
                )
                .expect("row policy authority"),
            )
            .expect("V4 grant");
        let (principal, current, environment) = facts(protected);
        let allowed_to_service = evaluate(
            &principal,
            &current,
            database_id(),
            &environment,
            timestamp(15),
            &OperationRequest::execute_ad_hoc_query(application_query_target()),
        );
        assert!(allowed_to_service.is_ok());
        let allowed_to_commit = evaluate(
            &principal,
            &current,
            database_id(),
            &environment,
            timestamp(15),
            &OperationRequest::execute_command(
                lineage(),
                ContractVersion::new(1).expect("version"),
                CommandId::first(),
                CommandExecutionClass::Mutation,
                partition(),
            ),
        );
        assert!(allowed_to_commit.is_ok());
        let allowed_to_live_query = evaluate(
            &principal,
            &current,
            database_id(),
            &environment,
            timestamp(15),
            &OperationRequest::watch_named_query(
                lineage(),
                reactive_module,
                reactive_operation,
                application_query_target(),
            )
            .expect("exact watch request"),
        );
        assert!(allowed_to_live_query.is_ok());
        let contextual_target = EventConsumerOperationTarget::new(
            lineage(),
            ContractVersion::new(1).expect("version"),
            ContractBundleHash::from_bytes([6; 32]),
            reactive_module,
            ReactiveOperationName::new("DocumentAgent").expect("operation"),
            QueryParameterHash::from_bytes([0x46; 32]),
            EventConsumerName::new("agent-1").expect("consumer"),
            OperationTenantScope::global_only(),
            partition(),
        );
        let allowed_to_contextual = evaluate(
            &principal,
            &current,
            database_id(),
            &environment,
            timestamp(15),
            &OperationRequest::consume_contextual_subscription(
                contextual_target.clone(),
                NonZeroU16::new(4).expect("rows"),
            ),
        );
        assert!(allowed_to_contextual.is_ok());
        let allowed_to_react = evaluate(
            &principal,
            &current,
            database_id(),
            &environment,
            timestamp(15),
            &OperationRequest::execute_contextual_reaction(contextual_target),
        );
        assert!(allowed_to_react.is_ok());
    }

    #[test]
    fn whole_query_budget_is_compared_once_across_repeated_steps() {
        let module_hash = QueryModuleHash::from_bytes([8; 32]);
        let query_name = QueryOperationName::new("TicketPage").expect("valid name");
        let (principal, current, environment) = facts(grant(
            TenantScope::Global,
            PartitionScopeV1::All,
            vec![CapabilityPermissionV1::ExecuteNamedQuery(
                lineage(),
                module_hash,
                query_name.clone(),
            )],
            vec![
                EntityFieldVisibilityV1::new(
                    lineage(),
                    EntityTypeId::first(),
                    vec![FieldId::first()],
                )
                .expect("field visibility"),
            ],
            10,
            Vec::new(),
        ));
        let repeated_target = |rows_per_step: u16, total: u64| {
            let access = || {
                ApplicationQueryAccessRequirement::new(
                    EntityTypeId::first(),
                    Some(IndexId::first()),
                    vec![FieldId::first()],
                    NonZeroU16::new(rows_per_step).expect("nonzero"),
                )
                .expect("valid access")
            };
            ApplicationQueryTarget::new(
                lineage(),
                ContractVersion::new(1).expect("nonzero version"),
                ContractBundleHash::from_bytes([6; 32]),
                QueryPlanHash::from_bytes([7; 32]),
                ServiceIngressKindV1::InProcessTestComparison,
                OperationTenantScope::global_only(),
                partition(),
                vec![access(), access()],
                riffdb_types::QueryCostVectorV1::new(2, total, total, 0, total, total, 1_024)
                    .expect("valid cost"),
            )
            .expect("valid repeated target")
        };
        let exact = OperationRequest::execute_named_query(
            lineage(),
            module_hash,
            query_name.clone(),
            repeated_target(5, 10),
        )
        .expect("matching exact target");
        assert!(
            evaluate(
                &principal,
                &current,
                current.database_id,
                &environment,
                timestamp(15),
                &exact,
            )
            .is_ok()
        );

        let amplified = OperationRequest::execute_named_query(
            lineage(),
            module_hash,
            query_name,
            repeated_target(6, 12),
        )
        .expect("matching amplified target");
        assert_eq!(
            evaluate(
                &principal,
                &current,
                current.database_id,
                &environment,
                timestamp(15),
                &amplified,
            ),
            Err(PolicyCode::MissingPermission)
        );
    }

    #[test]
    fn application_query_budget_distinguishes_scans_from_bounded_hydration() {
        let module_hash = QueryModuleHash::from_bytes([8; 32]);
        let query_name = QueryOperationName::new("InventoryDashboard").expect("valid name");
        let visibility =
            EntityFieldVisibilityV1::new(lineage(), EntityTypeId::first(), vec![FieldId::first()])
                .expect("field visibility");
        let (principal, current, environment) = facts(grant(
            TenantScope::Global,
            PartitionScopeV1::All,
            vec![CapabilityPermissionV1::ExecuteNamedQuery(
                lineage(),
                module_hash,
                query_name.clone(),
            )],
            vec![visibility],
            500,
            Vec::new(),
        ));
        let access = || {
            ApplicationQueryAccessRequirement::new(
                EntityTypeId::first(),
                Some(IndexId::first()),
                vec![FieldId::first()],
                NonZeroU16::new(500).expect("nonzero"),
            )
            .expect("valid access")
        };
        let target = ApplicationQueryTarget::new(
            lineage(),
            ContractVersion::new(1).expect("nonzero version"),
            ContractBundleHash::from_bytes([6; 32]),
            QueryPlanHash::from_bytes([7; 32]),
            ServiceIngressKindV1::InProcessTestComparison,
            OperationTenantScope::global_only(),
            partition(),
            vec![access(), access()],
            riffdb_types::QueryCostVectorV1::new(2, 500, 1_000, 500, 1_000, 2_000, 1_024)
                .expect("valid cost"),
        )
        .expect("valid target");
        let request =
            OperationRequest::execute_named_query(lineage(), module_hash, query_name, target)
                .expect("matching exact target");

        assert!(
            evaluate(
                &principal,
                &current,
                current.database_id,
                &environment,
                timestamp(15),
                &request,
            )
            .is_ok(),
            "a scan cap is not a total bounded hydration cap"
        );
    }

    #[test]
    fn application_query_requires_every_compiler_derived_visible_field() {
        let module_hash = QueryModuleHash::from_bytes([8; 32]);
        let query_name = QueryOperationName::new("TicketPage").expect("valid name");
        let (principal, current, environment) = facts(grant(
            TenantScope::Global,
            PartitionScopeV1::All,
            vec![CapabilityPermissionV1::ExecuteNamedQuery(
                lineage(),
                module_hash,
                query_name.clone(),
            )],
            Vec::new(),
            10,
            Vec::new(),
        ));
        let request = OperationRequest::execute_named_query(
            lineage(),
            module_hash,
            query_name,
            application_query_target(),
        )
        .expect("matching exact target");

        assert_eq!(
            evaluate(
                &principal,
                &current,
                current.database_id,
                &environment,
                timestamp(15),
                &request,
            ),
            Err(PolicyCode::MissingPermission)
        );
    }

    #[test]
    fn current_authorizer_returns_target_bound_create_preparation_only_with_trusted_config() {
        let database_id = database_id();
        let environment = Environment::new("dev").expect("valid environment");
        let audience = Audience::new("grpc").expect("valid audience");
        let parent_grant = grant(
            TenantScope::Global,
            PartitionScopeV1::All,
            vec![
                CapabilityPermissionV1::unparameterized(CapabilityPermissionKindV1::ReadHealth)
                    .expect("valid permission"),
                CapabilityPermissionV1::unparameterized(
                    CapabilityPermissionKindV1::CreateCapability,
                )
                .expect("valid permission"),
                CapabilityPermissionV1::unparameterized(
                    CapabilityPermissionKindV1::RevokeCapability,
                )
                .expect("valid permission"),
            ],
            Vec::new(),
            100,
            Vec::new(),
        );
        let fixture = AuthorizationFixture::new(AuthorizationFixtureConfig::new(
            database_id,
            environment.clone(),
            ActorId::new("principal-1").expect("valid actor"),
            ActorKind::Service,
            audience.clone(),
            AuthorizationFixtureTimes::new(timestamp(100), timestamp(1_000), timestamp(150)),
            parent_grant,
        ))
        .expect("valid fixture");
        let target_grant = grant(
            TenantScope::Global,
            PartitionScopeV1::All,
            vec![
                CapabilityPermissionV1::unparameterized(CapabilityPermissionKindV1::ReadHealth)
                    .expect("valid permission"),
            ],
            Vec::new(),
            10,
            Vec::new(),
        );
        let target = |target_audience: Audience| {
            let requested_record = crate::NormalizedCapabilityCreateRecord::new(
                database_id,
                environment.clone(),
                ActorId::new("target-principal").expect("valid actor"),
                ActorKind::Agent,
                NonZeroU32::new(60).expect("nonzero duration"),
                vec![target_audience],
                target_grant.clone(),
            )
            .expect("valid requested record");
            crate::CapabilityCreateTargetFacts::new(
                RequestId::from_unix_milliseconds_and_random(3, [0x31; 10]).expect("valid UUIDv7"),
                CapabilityId::from_unix_milliseconds_and_random(4, [0x41; 10])
                    .expect("valid UUIDv7"),
                requested_record,
            )
        };
        let resolver = fixture.current_capability_resolver();
        let clock = FixedAuthorizationClock(timestamp(200));
        let unconfigured = CurrentAuthorizer::new(
            &resolver,
            &clock,
            &crate::NoopAuthorizationTelemetry,
            database_id,
            environment.clone(),
        )
        .authorize(
            fixture.authenticated_principal(),
            OperationRequest::create_capability(target(audience.clone())),
        )
        .expect("policy decision");
        assert_eq!(
            unconfigured,
            Decision::Deny(PolicyCode::DelegationExceedsAuthority)
        );

        let catalog = crate::TrustedAudienceCatalog::new(vec![
            audience.clone(),
            Audience::new("mcp").expect("valid audience"),
        ])
        .expect("valid catalog");
        let configured = CurrentAuthorizer::new(
            &resolver,
            &clock,
            &crate::NoopAuthorizationTelemetry,
            database_id,
            environment.clone(),
        )
        .with_trusted_audience_catalog(&catalog)
        .authorize(
            fixture.authenticated_principal(),
            OperationRequest::create_capability(target(audience)),
        )
        .expect("policy decision");
        let Decision::PrepareCapabilityMutation(preparation) = configured else {
            panic!("expected create preparation");
        };
        assert_eq!(
            preparation.authorizing_capability_id(),
            fixture.authenticated_principal().capability_id()
        );
        assert_eq!(
            preparation.authorizing_revision(),
            fixture.authenticated_principal().capability_revision()
        );
        assert_eq!(preparation.validated_approval(), None);
        let prepared_target = preparation.create_target().expect("create target");
        assert_eq!(
            prepared_target.normalized_requested_record().database_id(),
            database_id
        );
        assert_eq!(
            prepared_target.normalized_requested_record().environment(),
            &environment
        );

        let outside_current_audience = CurrentAuthorizer::new(
            &resolver,
            &clock,
            &crate::NoopAuthorizationTelemetry,
            database_id,
            environment.clone(),
        )
        .with_trusted_audience_catalog(&catalog)
        .authorize(
            fixture.authenticated_principal(),
            OperationRequest::create_capability(target(
                Audience::new("mcp").expect("valid audience"),
            )),
        )
        .expect("policy decision");
        assert_eq!(
            outside_current_audience,
            Decision::Deny(PolicyCode::DelegationExceedsAuthority)
        );

        let revoke_target = crate::CapabilityRevokeTargetFacts::new(
            RequestId::from_unix_milliseconds_and_random(5, [0x51; 10]).expect("valid UUIDv7"),
            CapabilityId::from_unix_milliseconds_and_random(6, [0x61; 10]).expect("valid UUIDv7"),
            NonZeroU64::new(3).expect("nonzero revision"),
            crate::CapabilityActivity::Active,
            database_id,
            environment.clone(),
            ActorId::new("target-principal").expect("valid actor"),
            ActorKind::Agent,
            vec![Audience::new("grpc").expect("valid audience")],
            timestamp(100),
            timestamp(900),
            target_grant,
        )
        .expect("valid revoke target");
        let revoke = CurrentAuthorizer::new(
            &resolver,
            &clock,
            &crate::NoopAuthorizationTelemetry,
            database_id,
            environment,
        )
        .authorize(
            fixture.authenticated_principal(),
            OperationRequest::revoke_capability(
                revoke_target.clone(),
                crate::RevocationReasonCodeV1::Requested,
            ),
        )
        .expect("policy decision");
        let Decision::PrepareCapabilityMutation(preparation) = revoke else {
            panic!("expected revoke preparation");
        };
        assert_eq!(
            preparation.authorizing_capability_id(),
            fixture.authenticated_principal().capability_id()
        );
        assert_eq!(
            preparation.authorizing_revision(),
            fixture.authenticated_principal().capability_revision()
        );
        assert_eq!(preparation.validated_approval(), None);
        assert_eq!(preparation.revoke_target(), Some(&revoke_target));
        assert_eq!(
            preparation.revoke_reason(),
            Some(crate::RevocationReasonCodeV1::Requested)
        );
    }

    #[test]
    fn current_authorizer_exposes_absent_revoke_only_to_exact_scope_administrator() {
        let database_id = database_id();
        let environment = Environment::new("dev").expect("valid environment");
        let audience = Audience::new("grpc").expect("valid audience");
        let permission_grant = |kind| {
            grant(
                TenantScope::Global,
                PartitionScopeV1::All,
                vec![CapabilityPermissionV1::unparameterized(kind).expect("valid permission")],
                Vec::new(),
                1,
                Vec::new(),
            )
        };
        let target = crate::AbsentCapabilityRevokeTargetFacts::new(
            RequestId::from_unix_milliseconds_and_random(7, [0x71; 10]).expect("valid UUIDv7"),
            CapabilityId::from_unix_milliseconds_and_random(8, [0x81; 10]).expect("valid UUIDv7"),
            database_id,
            environment.clone(),
        );
        let authorize = |grant| {
            let fixture = AuthorizationFixture::new(AuthorizationFixtureConfig::new(
                database_id,
                environment.clone(),
                ActorId::new("principal-1").expect("valid actor"),
                ActorKind::Service,
                audience.clone(),
                AuthorizationFixtureTimes::new(timestamp(100), timestamp(1_000), timestamp(150)),
                grant,
            ))
            .expect("valid fixture");
            let resolver = fixture.current_capability_resolver();
            let capability_id = fixture.authenticated_principal().capability_id();
            let revision = fixture.authenticated_principal().capability_revision();
            let decision = CurrentAuthorizer::new(
                &resolver,
                &FixedAuthorizationClock(timestamp(200)),
                &crate::NoopAuthorizationTelemetry,
                database_id,
                environment.clone(),
            )
            .authorize(
                fixture.authenticated_principal(),
                OperationRequest::revoke_absent_capability(
                    target.clone(),
                    crate::RevocationReasonCodeV1::Requested,
                ),
            )
            .expect("policy decision");
            (decision, capability_id, revision)
        };

        assert_eq!(
            authorize(permission_grant(
                CapabilityPermissionKindV1::RevokeCapability
            ))
            .0,
            Decision::Deny(PolicyCode::MissingPermission)
        );
        let (decision, capability_id, revision) = authorize(permission_grant(
            CapabilityPermissionKindV1::AdministerCapabilities,
        ));
        let Decision::PrepareCapabilityMutation(preparation) = decision else {
            panic!("expected absent revoke preparation");
        };
        assert_eq!(preparation.authorizing_capability_id(), capability_id);
        assert_eq!(preparation.authorizing_revision(), revision);
        assert_eq!(preparation.validated_approval(), None);
        assert_eq!(preparation.absent_revoke_target(), Some(&target));

        let wrong_scope = crate::AbsentCapabilityRevokeTargetFacts::new(
            target.request_id(),
            target.capability_id(),
            DatabaseId::from_unix_milliseconds_and_random(9, [0x91; 10]).expect("valid UUIDv7"),
            environment.clone(),
        );
        let fixture = AuthorizationFixture::new(AuthorizationFixtureConfig::new(
            database_id,
            environment.clone(),
            ActorId::new("principal-1").expect("valid actor"),
            ActorKind::Service,
            audience,
            AuthorizationFixtureTimes::new(timestamp(100), timestamp(1_000), timestamp(150)),
            permission_grant(CapabilityPermissionKindV1::AdministerCapabilities),
        ))
        .expect("valid fixture");
        let resolver = fixture.current_capability_resolver();
        assert_eq!(
            CurrentAuthorizer::new(
                &resolver,
                &FixedAuthorizationClock(timestamp(200)),
                &crate::NoopAuthorizationTelemetry,
                database_id,
                environment,
            )
            .authorize(
                fixture.authenticated_principal(),
                OperationRequest::revoke_absent_capability(
                    wrong_scope,
                    crate::RevocationReasonCodeV1::Requested,
                ),
            )
            .expect("policy decision"),
            Decision::Deny(PolicyCode::DelegationExceedsAuthority)
        );
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
        assert_eq!(
            evaluate(
                &principal,
                &current,
                current.database_id,
                &environment,
                timestamp(15),
                &OperationRequest::resolve_command_outcome_pre_lookup(
                    lineage(),
                    CommandId::first(),
                ),
            ),
            Err(PolicyCode::TenantScopeMismatch)
        );
    }

    #[test]
    fn unfiltered_administrative_reads_require_global_tenant_and_all_partitions() {
        let cases = [
            (
                OperationRequest::get_commit(CommitSequence::first()),
                CapabilityPermissionKindV1::ReadCommit,
            ),
            (
                OperationRequest::scan_commits(NonZeroU16::new(10).expect("nonzero rows")),
                CapabilityPermissionKindV1::ScanCommits,
            ),
            (
                OperationRequest::subscribe_to_commits(),
                CapabilityPermissionKindV1::SubscribeCommits,
            ),
            (
                OperationRequest::trace_provenance(crate::ProvenanceSelector::Commit(
                    CommitSequence::first(),
                )),
                CapabilityPermissionKindV1::ReadProvenance,
            ),
            (
                OperationRequest::list_pending_outbox_deliveries(
                    NonZeroU16::new(10).expect("nonzero rows"),
                ),
                CapabilityPermissionKindV1::InspectOutbox,
            ),
        ];

        for (request, permission_kind) in cases {
            let permission = CapabilityPermissionV1::unparameterized(permission_kind)
                .expect("administrative permission");
            let tenant_grant = grant(
                TenantScope::Tenant(TenantId::new("tenant-a").expect("valid tenant")),
                PartitionScopeV1::All,
                vec![permission.clone()],
                Vec::new(),
                50,
                Vec::new(),
            );
            let (principal, current, environment) = facts(tenant_grant);
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

            let partition_grant = grant(
                TenantScope::Global,
                explicit_partition(),
                vec![permission.clone()],
                Vec::new(),
                50,
                Vec::new(),
            );
            let (principal, current, environment) = facts(partition_grant);
            assert_eq!(
                evaluate(
                    &principal,
                    &current,
                    current.database_id,
                    &environment,
                    timestamp(15),
                    &request,
                ),
                Err(PolicyCode::PartitionScopeMismatch)
            );

            let global_grant = grant(
                TenantScope::Global,
                PartitionScopeV1::All,
                vec![permission],
                Vec::new(),
                50,
                Vec::new(),
            );
            let (principal, current, environment) = facts(global_grant);
            let obligations = evaluate(
                &principal,
                &current,
                current.database_id,
                &environment,
                timestamp(15),
                &request,
            )
            .expect("global administrative read is allowed");
            assert_eq!(obligations.effective_tenant_scope(), &TenantScope::Global);
            assert_eq!(
                obligations.partition_constraint(),
                Some(&PartitionConstraint::Filter(PartitionScopeV1::All))
            );
        }
    }

    #[test]
    fn server_statistics_require_global_tenant_without_a_partition_constraint() {
        let permission =
            CapabilityPermissionV1::unparameterized(CapabilityPermissionKindV1::ReadStatistics)
                .expect("administrative permission");
        let tenant_grant = grant(
            TenantScope::Tenant(TenantId::new("tenant-a").expect("valid tenant")),
            PartitionScopeV1::All,
            vec![permission.clone()],
            Vec::new(),
            50,
            Vec::new(),
        );
        let (principal, current, environment) = facts(tenant_grant);
        assert_eq!(
            evaluate(
                &principal,
                &current,
                current.database_id,
                &environment,
                timestamp(15),
                &OperationRequest::get_statistics(),
            ),
            Err(PolicyCode::TenantScopeMismatch)
        );

        let global_grant = grant(
            TenantScope::Global,
            explicit_partition(),
            vec![permission],
            Vec::new(),
            50,
            Vec::new(),
        );
        let (principal, current, environment) = facts(global_grant);
        let obligations = evaluate(
            &principal,
            &current,
            current.database_id,
            &environment,
            timestamp(15),
            &OperationRequest::get_statistics(),
        )
        .expect("global server-statistics authority is allowed");
        assert_eq!(obligations.effective_tenant_scope(), &TenantScope::Global);
        assert_eq!(obligations.partition_constraint(), None);
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
    fn offline_maintenance_requires_global_current_administrator_authority() {
        let database_id = database_id();
        let environment = Environment::new("dev").expect("valid environment");
        let audience = Audience::new("grpc").expect("valid audience");
        let admin_permission = CapabilityPermissionV1::unparameterized(
            CapabilityPermissionKindV1::AdministerCapabilities,
        )
        .expect("valid administrative permission");
        let fixture = AuthorizationFixture::new(AuthorizationFixtureConfig::new(
            database_id,
            environment.clone(),
            ActorId::new("maintenance-principal").expect("valid actor"),
            ActorKind::Human,
            audience,
            AuthorizationFixtureTimes::new(timestamp(100), timestamp(1_000), timestamp(150)),
            grant(
                TenantScope::Global,
                explicit_partition(),
                vec![admin_permission],
                Vec::new(),
                5,
                Vec::new(),
            ),
        ))
        .expect("valid fixture");
        let resolver = fixture.current_capability_resolver();
        let clock = FixedAuthorizationClock(timestamp(200));
        let authorizer = CurrentAuthorizer::new(
            &resolver,
            &clock,
            &crate::NoopAuthorizationTelemetry,
            database_id,
            environment.clone(),
        );

        for (request, operation, input_hash) in [
            (
                OfflineMaintenanceAuthorizationRequest::create_backup(
                    maintenance_operation_id(),
                    maintenance_input_hash(),
                ),
                crate::OfflineMaintenancePolicyOperation::Start(
                    riffdb_types::OfflineMaintenanceOperationKind::CreateBackup,
                ),
                Some(maintenance_input_hash()),
            ),
            (
                OfflineMaintenanceAuthorizationRequest::restore_backup(
                    maintenance_operation_id(),
                    maintenance_input_hash(),
                ),
                crate::OfflineMaintenancePolicyOperation::Start(
                    riffdb_types::OfflineMaintenanceOperationKind::RestoreBackup,
                ),
                Some(maintenance_input_hash()),
            ),
            (
                OfflineMaintenanceAuthorizationRequest::get_operation(maintenance_operation_id()),
                crate::OfflineMaintenancePolicyOperation::GetOperation,
                None,
            ),
        ] {
            let decision = authorizer
                .authorize_offline_maintenance(fixture.authenticated_principal(), request)
                .expect("policy decision");
            let rendered_decision = format!("{decision:?}");
            let OfflineMaintenanceDecision::Allow(proof) = decision else {
                panic!("expected maintenance allow proof");
            };
            assert_eq!(proof.database_id(), database_id);
            assert_eq!(proof.environment(), &environment);
            assert_eq!(proof.request().operation_id(), maintenance_operation_id());
            assert_eq!(proof.operation(), operation);
            assert_eq!(proof.request().input_hash(), input_hash);
            assert_eq!(
                proof.authorizing_capability_id(),
                fixture.authenticated_principal().capability_id()
            );
            assert_eq!(
                proof.authorizing_capability_revision(),
                fixture.authenticated_principal().capability_revision()
            );
            assert_eq!(
                proof.principal_id(),
                fixture.authenticated_principal().principal_id()
            );
            assert_eq!(proof.actor_kind(), ActorKind::Human);
            assert_eq!(
                proof.obligations().effective_tenant_scope(),
                &TenantScope::Global
            );
            assert_eq!(proof.obligations().partition_constraint(), None);
            assert_eq!(proof.obligations().validated_approval(), None);
            assert_eq!(
                proof.obligations().output_classification(),
                OutputClassification::AdministrativeRedactedData
            );
            let rendered = format!("{rendered_decision} {proof:?}");
            assert!(rendered.contains("[REDACTED]"));
            assert!(!rendered.contains("maintenance-principal"));
        }
    }

    #[test]
    fn offline_maintenance_denies_missing_scope_approval_and_stale_state() {
        let admin_permission = CapabilityPermissionV1::unparameterized(
            CapabilityPermissionKindV1::AdministerCapabilities,
        )
        .expect("valid administrative permission");
        let read_permission =
            CapabilityPermissionV1::unparameterized(CapabilityPermissionKindV1::ReadHealth)
                .expect("valid read permission");
        let tenant = TenantScope::Tenant(TenantId::new("tenant-a").expect("valid tenant"));
        let cases = [
            (
                grant(
                    TenantScope::Global,
                    PartitionScopeV1::All,
                    vec![read_permission],
                    Vec::new(),
                    5,
                    Vec::new(),
                ),
                PolicyCode::MissingPermission,
            ),
            (
                grant(
                    tenant,
                    PartitionScopeV1::All,
                    vec![admin_permission.clone()],
                    Vec::new(),
                    5,
                    Vec::new(),
                ),
                PolicyCode::TenantScopeMismatch,
            ),
            (
                grant(
                    TenantScope::Global,
                    PartitionScopeV1::All,
                    vec![admin_permission],
                    Vec::new(),
                    5,
                    vec![CapabilityPermissionKindV1::AdministerCapabilities],
                ),
                PolicyCode::ApprovalRequired,
            ),
        ];

        for (grant, expected) in cases {
            let (principal, current, environment) = facts(grant);
            assert_eq!(
                evaluate_offline_maintenance(
                    &principal,
                    &current,
                    current.database_id,
                    &environment,
                    timestamp(15),
                ),
                Err(expected)
            );
        }

        let (principal, mut current, environment) = facts(grant(
            TenantScope::Global,
            PartitionScopeV1::All,
            vec![
                CapabilityPermissionV1::unparameterized(
                    CapabilityPermissionKindV1::AdministerCapabilities,
                )
                .expect("valid administrative permission"),
            ],
            Vec::new(),
            5,
            Vec::new(),
        ));
        current.activity = CurrentCapabilityActivity::Revoked;
        assert_eq!(
            evaluate_offline_maintenance(
                &principal,
                &current,
                current.database_id,
                &environment,
                timestamp(15),
            ),
            Err(PolicyCode::InactiveOrStaleCapability)
        );
    }

    #[test]
    fn contract_migration_proofs_bind_all_three_actions_and_exact_lineage() {
        let database_id = database_id();
        let environment = Environment::new("dev").expect("valid environment");
        let audience = Audience::new("grpc").expect("valid audience");
        let fixture = AuthorizationFixture::new(AuthorizationFixtureConfig::new(
            database_id,
            environment.clone(),
            ActorId::new("migration-principal").expect("valid actor"),
            ActorKind::Human,
            audience,
            AuthorizationFixtureTimes::new(timestamp(100), timestamp(1_000), timestamp(150)),
            grant(
                TenantScope::Global,
                PartitionScopeV1::All,
                vec![CapabilityPermissionV1::MigrateContract(lineage())],
                Vec::new(),
                5,
                Vec::new(),
            ),
        ))
        .expect("valid fixture");
        let resolver = fixture.current_capability_resolver();
        let clock = FixedAuthorizationClock(timestamp(200));
        let authorizer = CurrentAuthorizer::new(
            &resolver,
            &clock,
            &crate::NoopAuthorizationTelemetry,
            database_id,
            environment.clone(),
        );

        for request in [
            ContractMigrationAuthorizationRequest::start(
                migration_operation_id(),
                ContractMigrationPolicyOperation::Check,
                lineage(),
                migration_input_hash(),
            ),
            ContractMigrationAuthorizationRequest::start(
                migration_operation_id(),
                ContractMigrationPolicyOperation::Apply,
                lineage(),
                migration_input_hash(),
            ),
            ContractMigrationAuthorizationRequest::get_operation(
                migration_operation_id(),
                lineage(),
            ),
        ] {
            let expected_operation = request.operation();
            let expected_input_hash = request.input_hash();
            let decision = authorizer
                .authorize_contract_migration(fixture.authenticated_principal(), request)
                .expect("policy decision");
            let ContractMigrationDecision::Allow(proof) = decision else {
                panic!("expected migration allow proof");
            };
            assert_eq!(proof.database_id(), database_id);
            assert_eq!(proof.environment(), &environment);
            assert_eq!(proof.request().operation_id(), migration_operation_id());
            assert_eq!(proof.request().operation(), expected_operation);
            assert_eq!(proof.request().lineage(), &lineage());
            assert_eq!(proof.request().input_hash(), expected_input_hash);
            assert_eq!(
                proof.obligations().output_classification(),
                OutputClassification::AdministrativeRedactedData
            );
            assert!(format!("{proof:?}").contains("[REDACTED]"));
        }
    }

    #[test]
    fn application_installation_proofs_require_exact_lineage_and_operation() {
        let database_id = database_id();
        let environment = Environment::new("dev").expect("valid environment");
        let fixture = AuthorizationFixture::new(AuthorizationFixtureConfig::new(
            database_id,
            environment.clone(),
            ActorId::new("installer-principal").expect("valid actor"),
            ActorKind::Human,
            Audience::new("grpc").expect("valid audience"),
            AuthorizationFixtureTimes::new(timestamp(100), timestamp(1_000), timestamp(150)),
            grant(
                TenantScope::Global,
                PartitionScopeV1::All,
                vec![
                    CapabilityPermissionV1::InstallApplication(lineage()),
                    CapabilityPermissionV1::unparameterized(CapabilityPermissionKindV1::ReadHealth)
                        .expect("valid read permission"),
                ],
                Vec::new(),
                5,
                Vec::new(),
            ),
        ))
        .expect("valid fixture");
        let resolver = fixture.current_capability_resolver();
        let clock = FixedAuthorizationClock(timestamp(200));
        let authorizer = CurrentAuthorizer::new(
            &resolver,
            &clock,
            &crate::NoopAuthorizationTelemetry,
            database_id,
            environment.clone(),
        );

        for request in [
            OperationRequest::start_application_installation(lineage()),
            OperationRequest::get_application_installation(lineage()),
        ] {
            let expected_operation = request.operation();
            let Decision::Allow(proof) = authorizer
                .authorize(fixture.authenticated_principal(), request)
                .expect("policy decision")
            else {
                panic!("expected installation allow proof");
            };
            let proof = proof
                .into_application_installation()
                .expect("dedicated installation proof");
            assert_eq!(proof.database_id(), database_id);
            assert_eq!(proof.environment(), &environment);
            assert_eq!(proof.lineage(), &lineage());
            assert_eq!(proof.operation(), expected_operation);
            assert_eq!(
                proof.authorizing_capability_id(),
                fixture.authenticated_principal().capability_id()
            );
            assert_eq!(
                proof.obligations().effective_tenant_scope(),
                &TenantScope::Global
            );
            assert!(format!("{proof:?}").contains("[REDACTED]"));
        }

        let Decision::Allow(non_installation) = authorizer
            .authorize(
                fixture.authenticated_principal(),
                OperationRequest::get_health(),
            )
            .expect("policy decision")
        else {
            panic!("expected health allow proof");
        };
        assert!(
            non_installation.into_application_installation().is_none(),
            "ordinary authority must not become installation authority"
        );

        let decision = authorizer
            .authorize(
                fixture.authenticated_principal(),
                OperationRequest::start_application_installation(
                    ContractLineage::new("other.contract").expect("other lineage"),
                ),
            )
            .expect("policy decision");
        assert!(matches!(
            decision,
            Decision::Deny(PolicyCode::MissingPermission)
        ));
    }

    #[test]
    fn contract_migration_denies_implied_sibling_scoped_and_approval_authority() {
        let deploy =
            CapabilityPermissionV1::unparameterized(CapabilityPermissionKindV1::DeployContract)
                .expect("deploy permission");
        let migration = CapabilityPermissionV1::MigrateContract(lineage());
        let other_lineage = ContractLineage::new("other.contract").expect("other lineage");
        let tenant = TenantScope::Tenant(TenantId::new("tenant-a").expect("valid tenant"));
        let cases = [
            (
                grant(
                    TenantScope::Global,
                    PartitionScopeV1::All,
                    vec![deploy],
                    Vec::new(),
                    5,
                    Vec::new(),
                ),
                lineage(),
                PolicyCode::MissingPermission,
            ),
            (
                grant(
                    TenantScope::Global,
                    PartitionScopeV1::All,
                    vec![migration.clone()],
                    Vec::new(),
                    5,
                    Vec::new(),
                ),
                other_lineage,
                PolicyCode::MissingPermission,
            ),
            (
                grant(
                    tenant,
                    PartitionScopeV1::All,
                    vec![migration.clone()],
                    Vec::new(),
                    5,
                    Vec::new(),
                ),
                lineage(),
                PolicyCode::TenantScopeMismatch,
            ),
            (
                grant(
                    TenantScope::Global,
                    PartitionScopeV1::All,
                    vec![migration],
                    Vec::new(),
                    5,
                    vec![CapabilityPermissionKindV1::MigrateContract],
                ),
                lineage(),
                PolicyCode::ApprovalRequired,
            ),
        ];

        for (grant, requested_lineage, expected) in cases {
            let (principal, current, environment) = facts(grant);
            assert_eq!(
                evaluate_contract_migration(
                    &principal,
                    &current,
                    current.database_id,
                    &environment,
                    timestamp(15),
                    &requested_lineage,
                ),
                Err(expected)
            );
        }
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
        let pre_lookup =
            OperationRequest::resolve_command_outcome_pre_lookup(lineage(), CommandId::first());
        assert!(
            evaluate(
                &principal,
                &current,
                current.database_id,
                &environment,
                timestamp(15),
                &pre_lookup,
            )
            .is_ok()
        );
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
