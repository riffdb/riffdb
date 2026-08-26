//! Exact symbolic application roles compiled to private capability requirements.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::num::NonZeroU16;

use riffdb_contract_ir::{
    BindingMode, ContractBundle, PrincipalFactSchemaV1, RowPolicyExpressionNodeV1,
    RowPolicyOperationV1, RowPolicyPlanV1, RowPolicyValueSourceV1,
};
use riffdb_query_ir::{QueryAccessKind, ReactiveModulePlanV1, ReactiveOperationPlanV1};
use riffdb_types::{
    ActorId, ApplicationManifestHash, ApplicationRoleHash, CanonicalValue, CapabilityGrantV1,
    CapabilityPermissionKindV1, CapabilityPermissionV1, CapabilityPermissionsV1,
    CapabilityPrincipalFactsV1, CapabilityRowPolicyBindingV1, CapabilityRowPolicyGrantV1,
    CapabilityRowPolicyOperationV1, CapabilityVectorInspectionGrantV1,
    CapabilityVectorInspectionTargetV1, ContractBundleHash, ContractLineage, ContractVersion,
    EntityFieldVisibilityV1, Environment, PartitionScopeV1, QueryModuleHash, QueryOperationName,
    ReactiveModuleHash, RowPolicyName, TenantId, TenantScope, hash_application_role,
};

use crate::{ApplicationManifest, ManifestRole, ManifestTenantScope, QueryModule};

const ROLE_MAGIC: &[u8] = b"RIFFDB-APPLICATION-ROLE\0";
const ROLE_FORMAT_VERSION_V1: u32 = 1;
const ROLE_FORMAT_VERSION_V2: u32 = 2;
const ROLE_FORMAT_VERSION_V3: u32 = 3;
const ROLE_FORMAT_VERSION_V4: u32 = 4;
const ROLE_FORMAT_VERSION_V5: u32 = 5;
const MAX_ROLE_BYTES: usize = 1024 * 1024;

/// Symbolic operation kind exposed by a compiled application role.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ApplicationRoleOperationKind {
    /// One exact immutable named RiffQL query.
    Query,
    /// One exact compiled symbolic command.
    Command,
    /// One exact event stream.
    EventStream,
    /// One exact named-query watch.
    QueryWatch,
    /// One exact contextual subscription.
    AgentSubscription,
}

/// One safe name-only role operation description.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ApplicationRoleOperation {
    kind: ApplicationRoleOperationKind,
    name: String,
}

/// One symbolic secret-output authority atom derived from a selected query.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct ApplicationRoleSecretOutput {
    query: String,
    entity: String,
    entity_id: riffdb_types::EntityTypeId,
    field: String,
    field_id: riffdb_types::FieldId,
}

/// One exact compiler-derived vector-state inspection target.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct ApplicationRoleVectorInspection {
    entity: String,
    entity_id: riffdb_types::EntityTypeId,
    field: String,
    field_id: riffdb_types::FieldId,
    allow_counts: bool,
}

impl ApplicationRoleVectorInspection {
    /// Exact contract entity symbol.
    #[must_use]
    pub fn entity(&self) -> &str {
        &self.entity
    }

    /// Exact production vector-field symbol.
    #[must_use]
    pub fn field(&self) -> &str {
        &self.field
    }

    /// Whether this role may observe maintained whole-partition counts.
    #[must_use]
    pub const fn allow_counts(&self) -> bool {
        self.allow_counts
    }

    #[doc(hidden)]
    #[must_use]
    pub const fn internal_entity_id(&self) -> riffdb_types::EntityTypeId {
        self.entity_id
    }

    #[doc(hidden)]
    #[must_use]
    pub const fn internal_field_id(&self) -> riffdb_types::FieldId {
        self.field_id
    }
}

impl ApplicationRoleSecretOutput {
    /// Selected named query.
    #[must_use]
    pub fn query(&self) -> &str {
        &self.query
    }

    /// Exact contract entity symbol.
    #[must_use]
    pub fn entity(&self) -> &str {
        &self.entity
    }

    /// Exact secret field symbol.
    #[must_use]
    pub fn field(&self) -> &str {
        &self.field
    }

    /// Compiler-internal exact entity identity.
    #[doc(hidden)]
    #[must_use]
    pub const fn internal_entity_id(&self) -> riffdb_types::EntityTypeId {
        self.entity_id
    }

    /// Compiler-internal exact field identity.
    #[doc(hidden)]
    #[must_use]
    pub const fn internal_field_id(&self) -> riffdb_types::FieldId {
        self.field_id
    }
}

/// One safe name-only row-policy description attached to a compiled role.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ApplicationRolePolicy {
    name: String,
    entity: String,
    operations: Vec<RowPolicyOperationV1>,
}

impl ApplicationRolePolicy {
    /// Symbolic policy name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Symbolic protected entity name.
    #[must_use]
    pub fn entity(&self) -> &str {
        &self.entity
    }

    /// Closed operation rules present in the policy.
    #[must_use]
    pub fn operations(&self) -> &[RowPolicyOperationV1] {
        &self.operations
    }
}

/// One safe compiler-visible principal-fact requirement.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ApplicationRoleFactSchema {
    name: String,
    value_type: String,
    enum_variants: BTreeMap<String, CanonicalValue>,
}

impl ApplicationRoleFactSchema {
    /// Symbolic fact name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Symbolic public type spelling without stable numeric IDs.
    #[must_use]
    pub fn value_type(&self) -> &str {
        &self.value_type
    }

    /// Resolves one symbolic enum variant for trusted provisioning adapters.
    ///
    /// Stable enum IDs remain compiler-private; CLI and other operator surfaces
    /// accept only the contract symbol and cannot manufacture an enum identity.
    #[doc(hidden)]
    #[must_use]
    pub fn internal_resolve_enum_variant(&self, name: &str) -> Option<CanonicalValue> {
        self.enum_variants.get(name).cloned()
    }
}

impl ApplicationRoleOperation {
    /// Operation kind.
    #[must_use]
    pub const fn kind(&self) -> ApplicationRoleOperationKind {
        self.kind
    }

    /// Contract-defined operation name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }
}

/// A canonical role bound to one immutable application, contract, modules, environment, and scope.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CompiledApplicationRole {
    application_name: String,
    role_name: String,
    environment: Environment,
    tenant_scope: TenantScope,
    manifest_hash: ApplicationManifestHash,
    contract_lineage: ContractLineage,
    contract_version: ContractVersion,
    contract_hash: ContractBundleHash,
    module_hashes: Vec<QueryModuleHash>,
    reactive_module_hashes: Vec<ReactiveModuleHash>,
    operations: Vec<ApplicationRoleOperation>,
    secret_outputs: Vec<ApplicationRoleSecretOutput>,
    vector_inspections: Vec<ApplicationRoleVectorInspection>,
    row_policies: Vec<ApplicationRolePolicy>,
    principal_fact_schemas: Vec<ApplicationRoleFactSchema>,
    principal_fact_plans: Vec<PrincipalFactSchemaV1>,
    requires_uuid_principal: bool,
    policy_bindings: Vec<CapabilityRowPolicyBindingV1>,
    bound_permissions: CapabilityPermissionsV1,
    identity: ApplicationRoleHash,
    grant: CapabilityGrantV1,
}

impl CompiledApplicationRole {
    /// Application package name.
    #[must_use]
    pub fn application_name(&self) -> &str {
        &self.application_name
    }

    /// Symbolic role name.
    #[must_use]
    pub fn role_name(&self) -> &str {
        &self.role_name
    }

    /// Exact deployment environment.
    #[must_use]
    pub const fn environment(&self) -> &Environment {
        &self.environment
    }

    /// Exact tenant binding. Tenant values remain redacted by the domain type.
    #[must_use]
    pub const fn tenant_scope(&self) -> &TenantScope {
        &self.tenant_scope
    }

    /// Exact application-manifest identity.
    #[must_use]
    pub const fn manifest_hash(&self) -> ApplicationManifestHash {
        self.manifest_hash
    }

    /// Exact contract lineage.
    #[must_use]
    pub const fn contract_lineage(&self) -> &ContractLineage {
        &self.contract_lineage
    }

    /// Exact contract version.
    #[must_use]
    pub const fn contract_version(&self) -> ContractVersion {
        self.contract_version
    }

    /// Exact contract bundle hash.
    #[must_use]
    pub const fn contract_hash(&self) -> ContractBundleHash {
        self.contract_hash
    }

    /// Exact immutable query-module identities.
    #[must_use]
    pub fn module_hashes(&self) -> &[QueryModuleHash] {
        &self.module_hashes
    }
    /// Exact immutable reactive-module identities.
    #[must_use]
    pub fn reactive_module_hashes(&self) -> &[ReactiveModuleHash] {
        &self.reactive_module_hashes
    }

    /// Name-only operation allowlist suitable for CLI and MCP description.
    #[must_use]
    pub fn operations(&self) -> &[ApplicationRoleOperation] {
        &self.operations
    }

    /// Reviewed symbolic secret-output widenings derived from selected queries.
    #[must_use]
    pub fn secret_outputs(&self) -> &[ApplicationRoleSecretOutput] {
        &self.secret_outputs
    }

    /// Exact vector-state targets derived from the role's named nearest queries.
    #[must_use]
    pub fn vector_inspections(&self) -> &[ApplicationRoleVectorInspection] {
        &self.vector_inspections
    }

    /// Safe symbolic row-policy catalog for this exact role.
    #[must_use]
    pub fn row_policies(&self) -> &[ApplicationRolePolicy] {
        &self.row_policies
    }

    /// Safe required principal-fact schemas. Actual capability facts remain hidden.
    #[must_use]
    pub fn principal_fact_schemas(&self) -> &[ApplicationRoleFactSchema] {
        &self.principal_fact_schemas
    }

    /// Resolves a symbolic enum fact value against this exact compiled role.
    #[doc(hidden)]
    #[must_use]
    pub fn internal_resolve_principal_fact_enum(
        &self,
        fact_name: &str,
        variant_name: &str,
    ) -> Option<CanonicalValue> {
        self.principal_fact_schemas
            .iter()
            .find(|schema| schema.name() == fact_name)?
            .internal_resolve_enum_variant(variant_name)
    }

    /// Domain-separated role identity covering all public and private requirements.
    #[must_use]
    pub const fn identity(&self) -> ApplicationRoleHash {
        self.identity
    }

    /// Compiler-private capability lowering used only by trusted role binding.
    #[doc(hidden)]
    #[must_use]
    pub const fn internal_grant(&self) -> &CapabilityGrantV1 {
        &self.grant
    }

    /// Binds operator-supplied facts to this exact compiled role.
    ///
    /// This is a trusted provisioning operation: facts are checked against the
    /// compiler-retained schemas, and protected operation permissions become
    /// available only in the returned V4 grant that carries the matching role,
    /// fact, and policy identities.
    pub fn bind_principal_facts_for(
        &self,
        principal_id: &ActorId,
        facts: CapabilityPrincipalFactsV1,
    ) -> Result<CapabilityGrantV1, ApplicationRoleError> {
        if (self.requires_uuid_principal && !is_canonical_uuid(principal_id.as_str()))
            || self.principal_fact_plans.len() != facts.names().len()
            || self
                .principal_fact_plans
                .iter()
                .map(PrincipalFactSchemaV1::name)
                .ne(facts.names())
            || self.principal_fact_plans.iter().any(|schema| {
                facts.internal_fact(schema.name()).is_none_or(|fact| {
                    schema
                        .value_type()
                        .validate_value(fact.internal_value())
                        .is_err()
                })
            })
        {
            return Err(ApplicationRoleError::new(
                ApplicationRoleErrorKind::PrincipalFacts,
            ));
        }

        let mut grant = CapabilityGrantV1::new(
            self.tenant_scope.clone(),
            PartitionScopeV1::All,
            self.bound_permissions.clone(),
            self.grant.field_visibility().to_vec(),
            self.grant.max_scan_rows(),
            Vec::new(),
        )
        .map_err(|_| ApplicationRoleError::new(ApplicationRoleErrorKind::RequirementLimit))?;
        if !self.policy_bindings.is_empty() {
            let row_policy =
                CapabilityRowPolicyGrantV1::new(self.identity, facts, self.policy_bindings.clone())
                    .map_err(|_| {
                        ApplicationRoleError::new(ApplicationRoleErrorKind::RequirementLimit)
                    })?;
            grant = grant.with_row_policy(row_policy).map_err(|_| {
                ApplicationRoleError::new(ApplicationRoleErrorKind::RequirementLimit)
            })?;
        }
        if let Some(inspection) = self.grant.internal_vector_inspection() {
            grant = grant
                .with_vector_inspection(inspection.clone())
                .map_err(|_| {
                    ApplicationRoleError::new(ApplicationRoleErrorKind::RequirementLimit)
                })?;
        }
        Ok(grant)
    }
}

/// Closed role compilation failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ApplicationRoleErrorKind {
    /// The role name is not declared by the manifest.
    UnknownRole,
    /// A concrete tenant was supplied for a global role or omitted for a tenant role.
    TenantBindingMismatch,
    /// The contract differs from the exact manifest identity.
    ContractMismatch,
    /// A required module is absent, stale, duplicated, or bound to another contract.
    ModuleMismatch,
    /// The role names an unknown compiled command or query.
    UnknownOperation,
    /// A named policy is absent, duplicated by entity, or unavailable to this role schema.
    UnknownPolicy,
    /// One protected operation has no selected policy rule.
    PolicyCoverage,
    /// Bound principal facts are missing, extra, or do not match the compiled schemas.
    PrincipalFacts,
    /// A compiler-owned authority or cost bound cannot be represented safely.
    RequirementLimit,
}

/// Bounded redaction-safe role compilation error.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ApplicationRoleError {
    kind: ApplicationRoleErrorKind,
}

impl ApplicationRoleError {
    const fn new(kind: ApplicationRoleErrorKind) -> Self {
        Self { kind }
    }

    /// Stable failure kind.
    #[must_use]
    pub const fn kind(self) -> ApplicationRoleErrorKind {
        self.kind
    }
}

impl fmt::Display for ApplicationRoleError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self.kind {
            ApplicationRoleErrorKind::UnknownRole => {
                "application manifest does not declare the requested role"
            }
            ApplicationRoleErrorKind::TenantBindingMismatch => {
                "application role tenant binding does not match its declared scope"
            }
            ApplicationRoleErrorKind::ContractMismatch => {
                "application role contract identity is stale or substituted"
            }
            ApplicationRoleErrorKind::ModuleMismatch => {
                "application role query-module identity is missing, stale, or substituted"
            }
            ApplicationRoleErrorKind::UnknownOperation => {
                "application role names an operation absent from the exact compiled application"
            }
            ApplicationRoleErrorKind::UnknownPolicy => {
                "application role names an unavailable or ambiguous row policy"
            }
            ApplicationRoleErrorKind::PolicyCoverage => {
                "application role row policy does not cover a protected operation"
            }
            ApplicationRoleErrorKind::PrincipalFacts => {
                "application role principal facts do not match the compiled schemas"
            }
            ApplicationRoleErrorKind::RequirementLimit => {
                "application role derived authority exceeds a hard safety bound"
            }
        })
    }
}

impl std::error::Error for ApplicationRoleError {}

/// Compiles one manifest role and privately derives its least authority.
pub fn compile_application_role(
    manifest: &ApplicationManifest,
    role_name: &str,
    tenant: Option<TenantId>,
    contract: &ContractBundle,
    modules: &[QueryModule],
) -> Result<CompiledApplicationRole, ApplicationRoleError> {
    compile_application_role_inner(manifest, role_name, tenant, contract, modules, &[])
}

/// Compiles a V2 manifest role including exact reactive permissions.
pub fn compile_application_role_v2(
    manifest: &ApplicationManifest,
    role_name: &str,
    tenant: Option<TenantId>,
    contract: &ContractBundle,
    modules: &[QueryModule],
    reactive_modules: &[ReactiveModulePlanV1],
) -> Result<CompiledApplicationRole, ApplicationRoleError> {
    compile_application_role_inner(
        manifest,
        role_name,
        tenant,
        contract,
        modules,
        reactive_modules,
    )
}

fn compile_application_role_inner(
    manifest: &ApplicationManifest,
    role_name: &str,
    tenant: Option<TenantId>,
    contract: &ContractBundle,
    modules: &[QueryModule],
    reactive_modules: &[ReactiveModulePlanV1],
) -> Result<CompiledApplicationRole, ApplicationRoleError> {
    let role = manifest
        .roles()
        .iter()
        .find(|role| role.name() == role_name)
        .ok_or_else(|| ApplicationRoleError::new(ApplicationRoleErrorKind::UnknownRole))?;
    validate_contract(manifest, contract)?;
    let tenant_scope = bind_tenant(role, tenant)?;
    let environment = Environment::new(role.environment())
        .map_err(|_| ApplicationRoleError::new(ApplicationRoleErrorKind::RequirementLimit))?;
    let module_by_query = validate_modules(manifest, contract, modules)?;
    let (
        selected_policies,
        row_policies,
        principal_fact_schemas,
        principal_fact_plans,
        requires_uuid_principal,
    ) = compile_role_policies(manifest, role, contract)?;

    let lineage = contract.lineage().clone();
    // ADR-0056 makes contract-description access a compiler-derived part of
    // every symbolic application role. This is required for an application
    // driver to prove the exact active lineage/version/bundle before exposing
    // its generated operation catalog; callers never assemble this grant.
    let mut permissions = Vec::with_capacity(role.queries().len() + role.commands().len() + 1);
    let read_contract =
        CapabilityPermissionV1::Unparameterized(CapabilityPermissionKindV1::ReadContract);
    permissions.push(read_contract.clone());
    let mut bound_permissions = vec![read_contract];
    let mut fields_by_entity = BTreeMap::<_, BTreeSet<_>>::new();
    let mut secret_fields_by_entity = BTreeMap::<_, BTreeSet<_>>::new();
    let mut secret_output_atoms = BTreeSet::new();
    let mut vector_inspection_atoms = BTreeMap::new();
    let mut maximum_rows = 1_u64;
    let mut operations = Vec::with_capacity(role.queries().len() + role.commands().len());

    for query_name in role.queries() {
        let module = module_by_query
            .get(query_name.as_str())
            .ok_or_else(|| ApplicationRoleError::new(ApplicationRoleErrorKind::UnknownOperation))?;
        let query = module
            .query(query_name)
            .ok_or_else(|| ApplicationRoleError::new(ApplicationRoleErrorKind::UnknownOperation))?;
        let operation_name = QueryOperationName::new(query_name.clone())
            .map_err(|_| ApplicationRoleError::new(ApplicationRoleErrorKind::RequirementLimit))?;
        let mut requires_row_policy = false;
        maximum_rows = maximum_rows.max(minimum_scan_budget_for_cost(
            query.plan().authorization_cost(),
        ));
        for access in query.plan().authorization() {
            let entity = contract
                .schema()
                .entities()
                .iter()
                .find(|entity| entity.name() == access.entity())
                .ok_or_else(|| {
                    ApplicationRoleError::new(ApplicationRoleErrorKind::ContractMismatch)
                })?;
            require_policy_operation(
                contract,
                &selected_policies,
                entity.id(),
                RowPolicyOperationV1::Read,
            )?;
            requires_row_policy |= contract
                .row_policies()
                .policies()
                .iter()
                .any(|policy| policy.entity() == entity.id());
            let primary = entity
                .primary_key_fields()
                .iter()
                .copied()
                .collect::<BTreeSet<_>>();
            let fields = fields_by_entity.entry(entity.id()).or_default();
            fields.extend(
                access
                    .internal_fields()
                    .map(|(_, field)| field)
                    .filter(|field| !primary.contains(field)),
            );
        }
        for step in query.plan().representative_program().steps() {
            let QueryAccessKind::Nearest { vector_field, .. } = step.access() else {
                continue;
            };
            let entity = contract
                .schema()
                .entities()
                .iter()
                .find(|entity| entity.id() == step.internal_entity_id())
                .ok_or_else(|| {
                    ApplicationRoleError::new(ApplicationRoleErrorKind::ContractMismatch)
                })?;
            let field = entity
                .record()
                .fields()
                .iter()
                .find(|field| field.name() == vector_field)
                .ok_or_else(|| {
                    ApplicationRoleError::new(ApplicationRoleErrorKind::ContractMismatch)
                })?;
            if contract
                .schema()
                .vector_production_spec(entity.id(), field.id())
                .is_none()
            {
                return Err(ApplicationRoleError::new(
                    ApplicationRoleErrorKind::ContractMismatch,
                ));
            }
            fields_by_entity
                .entry(entity.id())
                .or_default()
                .insert(field.id());
            let allow_counts = !contract
                .row_policies()
                .policies()
                .iter()
                .any(|policy| policy.entity() == entity.id());
            vector_inspection_atoms.insert(
                (entity.id(), field.id()),
                ApplicationRoleVectorInspection {
                    entity: entity.name().to_owned(),
                    entity_id: entity.id(),
                    field: field.name().to_owned(),
                    field_id: field.id(),
                    allow_counts,
                },
            );
        }
        for requirement in query.plan().secret_outputs() {
            if !contract.schema().is_secret_field(
                requirement.internal_entity_id(),
                requirement.internal_field_id(),
            ) {
                return Err(ApplicationRoleError::new(
                    ApplicationRoleErrorKind::ContractMismatch,
                ));
            }
            secret_fields_by_entity
                .entry(requirement.internal_entity_id())
                .or_default()
                .insert(requirement.internal_field_id());
            let atom = (
                query_name.clone(),
                requirement.entity().to_owned(),
                requirement.internal_entity_id(),
                requirement.field().to_owned(),
                requirement.internal_field_id(),
            );
            if !secret_output_atoms.contains(&atom)
                && secret_output_atoms.len() == riffdb_riffql_syntax::MAX_COLLECTION_ITEMS
            {
                return Err(ApplicationRoleError::new(
                    ApplicationRoleErrorKind::RequirementLimit,
                ));
            }
            secret_output_atoms.insert(atom);
        }
        // WP-570 freezes the symbolic policy proof and identities. Until
        // WP-572 installs the shared transaction-current evaluator, granting
        // the ordinary operation permission would allow the existing runtime
        // to execute without the policy. Withhold it so partial rollout is a
        // closed authorization failure, never an application-side check.
        let permission = CapabilityPermissionV1::ExecuteNamedQuery(
            lineage.clone(),
            module.identity(),
            operation_name,
        );
        bound_permissions.push(permission.clone());
        if !requires_row_policy {
            permissions.push(permission);
        }
        operations.push(ApplicationRoleOperation {
            kind: ApplicationRoleOperationKind::Query,
            name: query_name.clone(),
        });
    }

    for command_name in role.commands() {
        let command = contract
            .commands()
            .iter()
            .find(|command| !command.is_reimport() && command.name() == command_name)
            .ok_or_else(|| ApplicationRoleError::new(ApplicationRoleErrorKind::UnknownOperation))?;
        let mut requires_row_policy = false;
        for binding in command.bindings() {
            let operations: &[RowPolicyOperationV1] = match binding.mode() {
                BindingMode::Read => &[RowPolicyOperationV1::Read],
                BindingMode::Mutate => &[RowPolicyOperationV1::Update],
                BindingMode::Create => &[RowPolicyOperationV1::Create],
                BindingMode::InitOrMutate => {
                    &[RowPolicyOperationV1::Create, RowPolicyOperationV1::Update]
                }
                BindingMode::Delete => &[RowPolicyOperationV1::Delete],
            };
            for operation in operations {
                require_policy_operation(
                    contract,
                    &selected_policies,
                    binding.entity_type(),
                    *operation,
                )?;
            }
            requires_row_policy |= contract
                .row_policies()
                .policies()
                .iter()
                .any(|policy| policy.entity() == binding.entity_type());
        }
        let permission =
            CapabilityPermissionV1::InvokeCommand(lineage.clone(), command.command_id());
        bound_permissions.push(permission.clone());
        if !requires_row_policy {
            permissions.push(permission);
        }
        operations.push(ApplicationRoleOperation {
            kind: ApplicationRoleOperationKind::Command,
            name: command_name.clone(),
        });
    }
    let reactive_by_operation = validate_reactive_modules(manifest, contract, reactive_modules)?;
    for (names, expected_kind) in [
        (
            role.event_streams(),
            ApplicationRoleOperationKind::EventStream,
        ),
        (
            role.watch_queries(),
            ApplicationRoleOperationKind::QueryWatch,
        ),
        (
            role.agent_subscriptions(),
            ApplicationRoleOperationKind::AgentSubscription,
        ),
    ] {
        for name in names {
            let (module, operation) = reactive_by_operation
                .get(name.as_str())
                .copied()
                .ok_or_else(|| {
                    ApplicationRoleError::new(ApplicationRoleErrorKind::UnknownOperation)
                })?;
            let kind_matches = matches!(
                (expected_kind, operation.plan()),
                (
                    ApplicationRoleOperationKind::EventStream,
                    ReactiveOperationPlanV1::Stream { .. }
                ) | (
                    ApplicationRoleOperationKind::QueryWatch,
                    ReactiveOperationPlanV1::Watch { .. }
                ) | (
                    ApplicationRoleOperationKind::AgentSubscription,
                    ReactiveOperationPlanV1::Subscription { .. }
                )
            );
            if !kind_matches {
                return Err(ApplicationRoleError::new(
                    ApplicationRoleErrorKind::UnknownOperation,
                ));
            }
            let permission = match expected_kind {
                ApplicationRoleOperationKind::EventStream => {
                    CapabilityPermissionV1::ConsumeEventStream(
                        lineage.clone(),
                        module.identity(),
                        operation.name().clone(),
                    )
                }
                ApplicationRoleOperationKind::QueryWatch => {
                    CapabilityPermissionV1::WatchNamedQuery(
                        lineage.clone(),
                        module.identity(),
                        operation.name().clone(),
                    )
                }
                ApplicationRoleOperationKind::AgentSubscription => {
                    CapabilityPermissionV1::ConsumeContextualSubscription(
                        lineage.clone(),
                        module.identity(),
                        operation.name().clone(),
                    )
                }
                ApplicationRoleOperationKind::Query | ApplicationRoleOperationKind::Command => {
                    unreachable!("reactive loop kind")
                }
            };
            // Reactive plans can disclose or hydrate protected rows. WP-572
            // owns their shared pre-shape enforcement, so a policy-bearing
            // contract cannot receive reactive execution authority early.
            bound_permissions.push(permission.clone());
            if contract.row_policies().is_empty() {
                permissions.push(permission);
            }
            operations.push(ApplicationRoleOperation {
                kind: expected_kind,
                name: name.clone(),
            });
        }
    }
    operations.sort_by(|left, right| {
        operation_kind_tag(left.kind)
            .cmp(&operation_kind_tag(right.kind))
            .then_with(|| left.name.cmp(&right.name))
    });

    let vector_inspections = vector_inspection_atoms.into_values().collect::<Vec<_>>();
    if !vector_inspections.is_empty() {
        let inspect =
            CapabilityPermissionV1::unparameterized(CapabilityPermissionKindV1::InspectVectorState)
                .map_err(|_| {
                    ApplicationRoleError::new(ApplicationRoleErrorKind::RequirementLimit)
                })?;
        permissions.push(inspect.clone());
        bound_permissions.push(inspect);
    }

    let permissions = CapabilityPermissionsV1::new(permissions)
        .map_err(|_| ApplicationRoleError::new(ApplicationRoleErrorKind::RequirementLimit))?;
    let secret_outputs = secret_output_atoms
        .into_iter()
        .map(
            |(query, entity, entity_id, field, field_id)| ApplicationRoleSecretOutput {
                query,
                entity,
                entity_id,
                field,
                field_id,
            },
        )
        .collect::<Vec<_>>();
    for (entity, secret_fields) in &secret_fields_by_entity {
        if let Some(ordinary_fields) = fields_by_entity.get_mut(entity) {
            ordinary_fields.retain(|field| !secret_fields.contains(field));
        }
    }
    let visibility_entities = fields_by_entity
        .keys()
        .chain(secret_fields_by_entity.keys())
        .copied()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .filter(|entity| {
            fields_by_entity
                .get(entity)
                .is_some_and(|fields| !fields.is_empty())
                || secret_fields_by_entity
                    .get(entity)
                    .is_some_and(|fields| !fields.is_empty())
        })
        .collect::<Vec<_>>();
    let field_visibility = visibility_entities
        .into_iter()
        .map(|entity| {
            EntityFieldVisibilityV1::with_secret_fields(
                lineage.clone(),
                entity,
                fields_by_entity
                    .remove(&entity)
                    .unwrap_or_default()
                    .into_iter()
                    .collect(),
                secret_fields_by_entity
                    .remove(&entity)
                    .unwrap_or_default()
                    .into_iter()
                    .collect(),
            )
        })
        .collect::<Result<Vec<_>, _>>()
        .map_err(|_| ApplicationRoleError::new(ApplicationRoleErrorKind::RequirementLimit))?;
    let maximum_rows = u16::try_from(maximum_rows)
        .ok()
        .and_then(NonZeroU16::new)
        .filter(|value| value.get() <= 500)
        .ok_or_else(|| ApplicationRoleError::new(ApplicationRoleErrorKind::RequirementLimit))?;
    let base_grant = CapabilityGrantV1::new(
        tenant_scope.clone(),
        PartitionScopeV1::All,
        permissions,
        field_visibility,
        maximum_rows,
        Vec::new(),
    )
    .map_err(|_| ApplicationRoleError::new(ApplicationRoleErrorKind::RequirementLimit))?;

    let mut module_hashes = modules
        .iter()
        .map(QueryModule::identity)
        .collect::<Vec<_>>();
    module_hashes.sort_unstable();
    let mut reactive_module_hashes = reactive_modules
        .iter()
        .map(ReactiveModulePlanV1::identity)
        .collect::<Vec<_>>();
    reactive_module_hashes.sort_unstable();
    let canonical = encode_role(
        manifest,
        role,
        &environment,
        &tenant_scope,
        contract,
        &module_hashes,
        &reactive_module_hashes,
        &operations,
        &row_policies,
        &principal_fact_schemas,
        &secret_outputs,
        &vector_inspections,
        &base_grant,
    )?;
    let identity = hash_application_role(&canonical);
    let mut final_permissions = base_grant.permissions().as_slice().to_vec();
    final_permissions.push(CapabilityPermissionV1::ApplicationRoleIdentity(identity));
    bound_permissions.push(CapabilityPermissionV1::ApplicationRoleIdentity(identity));
    let bound_permissions = CapabilityPermissionsV1::new(bound_permissions)
        .map_err(|_| ApplicationRoleError::new(ApplicationRoleErrorKind::RequirementLimit))?;
    let mut grant = CapabilityGrantV1::new(
        tenant_scope.clone(),
        PartitionScopeV1::All,
        CapabilityPermissionsV1::new(final_permissions)
            .map_err(|_| ApplicationRoleError::new(ApplicationRoleErrorKind::RequirementLimit))?,
        base_grant.field_visibility().to_vec(),
        base_grant.max_scan_rows(),
        Vec::new(),
    )
    .map_err(|_| ApplicationRoleError::new(ApplicationRoleErrorKind::RequirementLimit))?;
    if !vector_inspections.is_empty() {
        grant = grant
            .with_vector_inspection(
                CapabilityVectorInspectionGrantV1::new(
                    identity,
                    vector_inspections
                        .iter()
                        .map(|target| {
                            CapabilityVectorInspectionTargetV1::new(
                                lineage.clone(),
                                target.internal_entity_id(),
                                target.internal_field_id(),
                                target.allow_counts(),
                            )
                        })
                        .collect(),
                )
                .map_err(|_| {
                    ApplicationRoleError::new(ApplicationRoleErrorKind::RequirementLimit)
                })?,
            )
            .map_err(|_| ApplicationRoleError::new(ApplicationRoleErrorKind::RequirementLimit))?;
    }
    let policy_bindings = compile_policy_bindings(&selected_policies, &lineage)?;
    Ok(CompiledApplicationRole {
        application_name: manifest.application_name().to_owned(),
        role_name: role.name().to_owned(),
        environment,
        tenant_scope,
        manifest_hash: manifest.identity(),
        contract_lineage: lineage,
        contract_version: contract.contract_version(),
        contract_hash: contract.bundle_hash(),
        module_hashes,
        reactive_module_hashes,
        operations,
        secret_outputs,
        vector_inspections,
        row_policies,
        principal_fact_schemas,
        principal_fact_plans,
        requires_uuid_principal,
        policy_bindings,
        bound_permissions,
        identity,
        grant,
    })
}

fn minimum_scan_budget_for_cost(cost: riffdb_types::QueryCostVectorV1) -> u64 {
    let row_work_factor = riffdb_types::MAX_APPLICATION_QUERY_STEPS;
    let projected_value_factor =
        row_work_factor.saturating_mul(riffdb_types::MAX_CAPABILITY_FIELD_VISIBILITY as u64);
    [
        cost.scanned_index_rows(),
        cost.point_reads().div_ceil(row_work_factor),
        cost.dependent_keys().div_ceil(row_work_factor),
        cost.intermediate_rows().div_ceil(row_work_factor),
        cost.projected_values().div_ceil(projected_value_factor),
    ]
    .into_iter()
    .max()
    .unwrap_or(1)
    .max(1)
}

type SelectedPolicyMap<'a> = BTreeMap<riffdb_types::EntityTypeId, &'a RowPolicyPlanV1>;
type CompiledRolePolicies<'a> = (
    SelectedPolicyMap<'a>,
    Vec<ApplicationRolePolicy>,
    Vec<ApplicationRoleFactSchema>,
    Vec<PrincipalFactSchemaV1>,
    bool,
);

fn compile_role_policies<'a>(
    manifest: &ApplicationManifest,
    role: &ManifestRole,
    contract: &'a ContractBundle,
) -> Result<CompiledRolePolicies<'a>, ApplicationRoleError> {
    let catalog = contract.row_policies();
    if manifest.schema() != crate::APPLICATION_MANIFEST_SCHEMA_V4 {
        if !catalog.is_empty() {
            return Err(ApplicationRoleError::new(
                ApplicationRoleErrorKind::ContractMismatch,
            ));
        }
        return Ok((BTreeMap::new(), Vec::new(), Vec::new(), Vec::new(), false));
    }

    let mut selected = BTreeMap::new();
    let mut descriptions = Vec::with_capacity(role.row_policies().len());
    let mut fact_names = BTreeSet::new();
    for name in role.row_policies() {
        let policy = catalog
            .policies()
            .iter()
            .find(|policy| policy.name() == name)
            .ok_or_else(|| ApplicationRoleError::new(ApplicationRoleErrorKind::UnknownPolicy))?;
        if selected.insert(policy.entity(), policy).is_some() {
            return Err(ApplicationRoleError::new(
                ApplicationRoleErrorKind::UnknownPolicy,
            ));
        }
        collect_policy_fact_names(policy, &mut fact_names);
        let entity = contract
            .schema()
            .entity(policy.entity())
            .ok_or_else(|| ApplicationRoleError::new(ApplicationRoleErrorKind::ContractMismatch))?;
        descriptions.push(ApplicationRolePolicy {
            name: policy.name().to_owned(),
            entity: entity.name().to_owned(),
            operations: policy.rules().iter().map(|rule| rule.operation()).collect(),
        });
    }
    descriptions.sort_by(|left, right| left.name.cmp(&right.name));

    let facts_by_name = catalog
        .facts()
        .iter()
        .map(|fact| (fact.name(), fact))
        .collect::<BTreeMap<_, _>>();
    let principal_facts = fact_names
        .into_iter()
        .map(|name| {
            let fact = facts_by_name.get(name.as_str()).ok_or_else(|| {
                ApplicationRoleError::new(ApplicationRoleErrorKind::ContractMismatch)
            })?;
            Ok((
                ApplicationRoleFactSchema {
                    name,
                    value_type: render_fact_type(fact, contract)?,
                    enum_variants: compile_fact_enum_variants(fact, contract)?,
                },
                (*fact).clone(),
            ))
        })
        .collect::<Result<Vec<_>, ApplicationRoleError>>()?;
    let (principal_fact_schemas, principal_fact_plans) = principal_facts.into_iter().unzip();
    let requires_uuid_principal = selected.values().any(|policy| {
        policy.rules().iter().any(|rule| {
            rule.nodes().iter().any(|node| match node {
                RowPolicyExpressionNodeV1::Operand(operand) => {
                    matches!(operand.source(), RowPolicyValueSourceV1::PrincipalId)
                }
                RowPolicyExpressionNodeV1::IndexedExists { arguments, .. } => {
                    arguments.iter().any(|argument| {
                        matches!(argument.source(), RowPolicyValueSourceV1::PrincipalId)
                    })
                }
                _ => false,
            })
        })
    });
    Ok((
        selected,
        descriptions,
        principal_fact_schemas,
        principal_fact_plans,
        requires_uuid_principal,
    ))
}

fn is_canonical_uuid(value: &str) -> bool {
    value.len() == 36
        && value.bytes().enumerate().all(|(index, byte)| {
            if matches!(index, 8 | 13 | 18 | 23) {
                byte == b'-'
            } else {
                byte.is_ascii_digit() || matches!(byte, b'a'..=b'f')
            }
        })
}

fn compile_policy_bindings(
    selected: &SelectedPolicyMap<'_>,
    lineage: &ContractLineage,
) -> Result<Vec<CapabilityRowPolicyBindingV1>, ApplicationRoleError> {
    selected
        .values()
        .map(|policy| {
            let operations = policy
                .rules()
                .iter()
                .map(|rule| match rule.operation() {
                    RowPolicyOperationV1::Read => CapabilityRowPolicyOperationV1::Read,
                    RowPolicyOperationV1::Create => CapabilityRowPolicyOperationV1::Create,
                    RowPolicyOperationV1::Update => CapabilityRowPolicyOperationV1::Update,
                    RowPolicyOperationV1::Delete => CapabilityRowPolicyOperationV1::Delete,
                })
                .collect();
            CapabilityRowPolicyBindingV1::new(
                lineage.clone(),
                RowPolicyName::new(policy.name()).map_err(|_| {
                    ApplicationRoleError::new(ApplicationRoleErrorKind::ContractMismatch)
                })?,
                policy.entity(),
                operations,
            )
            .map_err(|_| ApplicationRoleError::new(ApplicationRoleErrorKind::RequirementLimit))
        })
        .collect()
}

fn collect_policy_fact_names(policy: &RowPolicyPlanV1, names: &mut BTreeSet<String>) {
    for rule in policy.rules() {
        for node in rule.nodes() {
            match node {
                RowPolicyExpressionNodeV1::Operand(operand) => {
                    collect_operand_fact_name(operand.source(), names);
                }
                RowPolicyExpressionNodeV1::IndexedExists { arguments, .. } => {
                    for argument in arguments {
                        collect_operand_fact_name(argument.source(), names);
                    }
                }
                RowPolicyExpressionNodeV1::Equal { .. }
                | RowPolicyExpressionNodeV1::NotEqual { .. }
                | RowPolicyExpressionNodeV1::Not { .. }
                | RowPolicyExpressionNodeV1::And { .. }
                | RowPolicyExpressionNodeV1::Or { .. }
                | RowPolicyExpressionNodeV1::In { .. }
                | RowPolicyExpressionNodeV1::IsNull { .. } => {}
            }
        }
    }
}

fn collect_operand_fact_name(source: &RowPolicyValueSourceV1, names: &mut BTreeSet<String>) {
    if let RowPolicyValueSourceV1::PrincipalFact(name) = source {
        names.insert(name.clone());
    }
}

fn require_policy_operation(
    contract: &ContractBundle,
    selected: &SelectedPolicyMap<'_>,
    entity: riffdb_types::EntityTypeId,
    operation: RowPolicyOperationV1,
) -> Result<(), ApplicationRoleError> {
    let protected = contract
        .row_policies()
        .policies()
        .iter()
        .any(|policy| policy.entity() == entity);
    if !protected {
        return Ok(());
    }
    let policy = selected
        .get(&entity)
        .ok_or_else(|| ApplicationRoleError::new(ApplicationRoleErrorKind::PolicyCoverage))?;
    if policy
        .rules()
        .iter()
        .any(|rule| rule.operation() == operation)
    {
        Ok(())
    } else {
        Err(ApplicationRoleError::new(
            ApplicationRoleErrorKind::PolicyCoverage,
        ))
    }
}

fn render_fact_type(
    fact: &riffdb_contract_ir::PrincipalFactSchemaV1,
    contract: &ContractBundle,
) -> Result<String, ApplicationRoleError> {
    fact.public_type_name(contract.schema())
        .map_err(|_| ApplicationRoleError::new(ApplicationRoleErrorKind::ContractMismatch))
}

fn compile_fact_enum_variants(
    fact: &PrincipalFactSchemaV1,
    contract: &ContractBundle,
) -> Result<BTreeMap<String, CanonicalValue>, ApplicationRoleError> {
    let scalar = fact
        .value_type()
        .list_parts()
        .map_or(fact.value_type(), |(element, _)| element);
    let Some(type_id) = scalar.enum_type_id() else {
        return Ok(BTreeMap::new());
    };
    let enumeration = contract
        .schema()
        .enumeration(type_id)
        .ok_or_else(|| ApplicationRoleError::new(ApplicationRoleErrorKind::ContractMismatch))?;
    Ok(enumeration
        .variants()
        .iter()
        .map(|variant| {
            (
                variant.name().to_owned(),
                CanonicalValue::Enum {
                    type_id: enumeration.id(),
                    variant_id: variant.id(),
                },
            )
        })
        .collect())
}

fn validate_reactive_modules<'a>(
    manifest: &ApplicationManifest,
    contract: &ContractBundle,
    modules: &'a [ReactiveModulePlanV1],
) -> Result<
    BTreeMap<
        String,
        (
            &'a ReactiveModulePlanV1,
            &'a riffdb_query_ir::CompiledReactiveOperationV1,
        ),
    >,
    ApplicationRoleError,
> {
    if modules.len() != manifest.reactive_modules().len() {
        return if modules.is_empty() && manifest.reactive_modules().is_empty() {
            Ok(BTreeMap::new())
        } else {
            Err(ApplicationRoleError::new(
                ApplicationRoleErrorKind::ModuleMismatch,
            ))
        };
    }
    let mut operations = BTreeMap::new();
    for declaration in manifest.reactive_modules() {
        let module = modules
            .iter()
            .find(|module| module.name() == declaration.name())
            .ok_or_else(|| ApplicationRoleError::new(ApplicationRoleErrorKind::ModuleMismatch))?;
        if module.identity() != declaration.module_hash()
            || module.version() != declaration.version()
            || module.contract_hash() != contract.bundle_hash()
        {
            return Err(ApplicationRoleError::new(
                ApplicationRoleErrorKind::ModuleMismatch,
            ));
        }
        for operation in module.operations() {
            if operations
                .insert(operation.name().as_str().to_owned(), (module, operation))
                .is_some()
            {
                return Err(ApplicationRoleError::new(
                    ApplicationRoleErrorKind::ModuleMismatch,
                ));
            }
        }
    }
    Ok(operations)
}

fn validate_contract(
    manifest: &ApplicationManifest,
    contract: &ContractBundle,
) -> Result<(), ApplicationRoleError> {
    let expected = manifest.contract();
    if expected.lineage() != contract.lineage().as_str()
        || expected.version() != contract.contract_version().get()
        || expected.bundle_hash() != contract.bundle_hash()
    {
        return Err(ApplicationRoleError::new(
            ApplicationRoleErrorKind::ContractMismatch,
        ));
    }
    Ok(())
}

fn bind_tenant(
    role: &ManifestRole,
    tenant: Option<TenantId>,
) -> Result<TenantScope, ApplicationRoleError> {
    match (role.tenant_scope(), tenant) {
        (ManifestTenantScope::Global, None) => Ok(TenantScope::Global),
        (ManifestTenantScope::Tenant, Some(tenant)) => Ok(TenantScope::Tenant(tenant)),
        _ => Err(ApplicationRoleError::new(
            ApplicationRoleErrorKind::TenantBindingMismatch,
        )),
    }
}

fn validate_modules<'a>(
    manifest: &ApplicationManifest,
    contract: &ContractBundle,
    modules: &'a [QueryModule],
) -> Result<BTreeMap<String, &'a QueryModule>, ApplicationRoleError> {
    if modules.len() != manifest.query_modules().len() {
        return Err(ApplicationRoleError::new(
            ApplicationRoleErrorKind::ModuleMismatch,
        ));
    }
    let mut by_query = BTreeMap::new();
    for declaration in manifest.query_modules() {
        let module = modules
            .iter()
            .find(|module| module.name().as_str() == declaration.name())
            .ok_or_else(|| ApplicationRoleError::new(ApplicationRoleErrorKind::ModuleMismatch))?;
        if module.identity() != declaration.module_hash()
            || module.version().get() != declaration.version()
            || module.contract_hash() != contract.bundle_hash()
        {
            return Err(ApplicationRoleError::new(
                ApplicationRoleErrorKind::ModuleMismatch,
            ));
        }
        for query in declaration.queries() {
            if module.query(query.name()).is_none()
                || by_query.insert(query.name().to_owned(), module).is_some()
            {
                return Err(ApplicationRoleError::new(
                    ApplicationRoleErrorKind::ModuleMismatch,
                ));
            }
        }
    }
    Ok(by_query)
}

#[allow(clippy::too_many_arguments)]
fn encode_role(
    manifest: &ApplicationManifest,
    role: &ManifestRole,
    environment: &Environment,
    tenant_scope: &TenantScope,
    contract: &ContractBundle,
    module_hashes: &[QueryModuleHash],
    reactive_module_hashes: &[ReactiveModuleHash],
    operations: &[ApplicationRoleOperation],
    row_policies: &[ApplicationRolePolicy],
    principal_fact_schemas: &[ApplicationRoleFactSchema],
    secret_outputs: &[ApplicationRoleSecretOutput],
    vector_inspections: &[ApplicationRoleVectorInspection],
    grant: &CapabilityGrantV1,
) -> Result<Vec<u8>, ApplicationRoleError> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(ROLE_MAGIC);
    bytes.extend_from_slice(
        &(if !vector_inspections.is_empty() {
            ROLE_FORMAT_VERSION_V5
        } else if !secret_outputs.is_empty() {
            ROLE_FORMAT_VERSION_V4
        } else if manifest.schema() == crate::APPLICATION_MANIFEST_SCHEMA_V4 {
            ROLE_FORMAT_VERSION_V3
        } else if reactive_module_hashes.is_empty() {
            ROLE_FORMAT_VERSION_V1
        } else {
            ROLE_FORMAT_VERSION_V2
        })
        .to_be_bytes(),
    );
    write_bytes(&mut bytes, manifest.identity().as_bytes())?;
    write_text(&mut bytes, manifest.application_name())?;
    write_text(&mut bytes, role.name())?;
    write_text(&mut bytes, environment.as_str())?;
    write_bytes(&mut bytes, &tenant_scope.to_canonical_bytes())?;
    write_text(&mut bytes, contract.lineage().as_str())?;
    bytes.extend_from_slice(&contract.contract_version().get().to_be_bytes());
    bytes.extend_from_slice(contract.bundle_hash().as_bytes());
    write_count(&mut bytes, module_hashes.len())?;
    for hash in module_hashes {
        bytes.extend_from_slice(hash.as_bytes());
    }
    if !reactive_module_hashes.is_empty() {
        write_count(&mut bytes, reactive_module_hashes.len())?;
        for hash in reactive_module_hashes {
            bytes.extend_from_slice(hash.as_bytes());
        }
    }
    write_count(&mut bytes, operations.len())?;
    for operation in operations {
        bytes.push(operation_kind_tag(operation.kind));
        write_text(&mut bytes, operation.name())?;
    }
    if manifest.schema() == crate::APPLICATION_MANIFEST_SCHEMA_V4 {
        write_count(&mut bytes, row_policies.len())?;
        for policy in row_policies {
            write_text(&mut bytes, policy.name())?;
            write_text(&mut bytes, policy.entity())?;
            write_count(&mut bytes, policy.operations().len())?;
            for operation in policy.operations() {
                bytes.push(row_policy_operation_tag(*operation));
            }
        }
        write_count(&mut bytes, principal_fact_schemas.len())?;
        for fact in principal_fact_schemas {
            write_text(&mut bytes, fact.name())?;
            write_text(&mut bytes, fact.value_type())?;
        }
    }
    for permission in grant.permissions().as_slice() {
        write_bytes(&mut bytes, &permission.canonical_key())?;
    }
    if secret_outputs.is_empty() {
        for visibility in grant.field_visibility() {
            bytes.extend_from_slice(&visibility.entity_type().to_be_bytes());
            for field in visibility.fields() {
                bytes.extend_from_slice(&field.to_be_bytes());
            }
        }
    } else {
        write_count(&mut bytes, grant.field_visibility().len())?;
        for visibility in grant.field_visibility() {
            bytes.extend_from_slice(&visibility.entity_type().to_be_bytes());
            write_count(&mut bytes, visibility.fields().len())?;
            for field in visibility.fields() {
                bytes.extend_from_slice(&field.to_be_bytes());
            }
            write_count(&mut bytes, visibility.secret_fields().len())?;
            for field in visibility.secret_fields() {
                bytes.extend_from_slice(&field.to_be_bytes());
            }
        }
        write_count(&mut bytes, secret_outputs.len())?;
        for output in secret_outputs {
            write_text(&mut bytes, output.query())?;
            write_text(&mut bytes, output.entity())?;
            bytes.extend_from_slice(&output.internal_entity_id().to_be_bytes());
            write_text(&mut bytes, output.field())?;
            bytes.extend_from_slice(&output.internal_field_id().to_be_bytes());
        }
    }
    if !vector_inspections.is_empty() {
        write_count(&mut bytes, vector_inspections.len())?;
        for inspection in vector_inspections {
            write_text(&mut bytes, inspection.entity())?;
            bytes.extend_from_slice(&inspection.internal_entity_id().to_be_bytes());
            write_text(&mut bytes, inspection.field())?;
            bytes.extend_from_slice(&inspection.internal_field_id().to_be_bytes());
            bytes.push(u8::from(inspection.allow_counts()));
        }
    }
    bytes.extend_from_slice(&grant.max_scan_rows().get().to_be_bytes());
    if bytes.len() > MAX_ROLE_BYTES {
        return Err(ApplicationRoleError::new(
            ApplicationRoleErrorKind::RequirementLimit,
        ));
    }
    Ok(bytes)
}

const fn row_policy_operation_tag(operation: RowPolicyOperationV1) -> u8 {
    match operation {
        RowPolicyOperationV1::Read => 1,
        RowPolicyOperationV1::Create => 2,
        RowPolicyOperationV1::Update => 3,
        RowPolicyOperationV1::Delete => 4,
    }
}

const fn operation_kind_tag(kind: ApplicationRoleOperationKind) -> u8 {
    match kind {
        ApplicationRoleOperationKind::Query => 1,
        ApplicationRoleOperationKind::Command => 2,
        ApplicationRoleOperationKind::EventStream => 3,
        ApplicationRoleOperationKind::QueryWatch => 4,
        ApplicationRoleOperationKind::AgentSubscription => 5,
    }
}

fn write_text(bytes: &mut Vec<u8>, value: &str) -> Result<(), ApplicationRoleError> {
    write_bytes(bytes, value.as_bytes())
}

fn write_bytes(bytes: &mut Vec<u8>, value: &[u8]) -> Result<(), ApplicationRoleError> {
    write_count(bytes, value.len())?;
    bytes.extend_from_slice(value);
    Ok(())
}

fn write_count(bytes: &mut Vec<u8>, value: usize) -> Result<(), ApplicationRoleError> {
    bytes.extend_from_slice(
        &u32::try_from(value)
            .map_err(|_| ApplicationRoleError::new(ApplicationRoleErrorKind::RequirementLimit))?
            .to_be_bytes(),
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn role_scan_budget_covers_every_request_time_cost_dimension() {
        let cost =
            riffdb_types::QueryCostVectorV1::new(1, 0, 65, 129, 257, 1, 1_024).expect("cost");
        assert_eq!(minimum_scan_budget_for_cost(cost), 5);
    }
}
