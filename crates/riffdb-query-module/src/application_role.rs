//! Exact symbolic application roles compiled to private capability requirements.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::num::NonZeroU16;

use riffdb_contract_ir::{
    BindingMode, ContractBundle, RowPolicyExpressionNodeV1, RowPolicyOperationV1, RowPolicyPlanV1,
    RowPolicyValueSourceV1,
};
use riffdb_query_ir::{ReactiveModulePlanV1, ReactiveOperationPlanV1};
use riffdb_types::{
    ApplicationManifestHash, ApplicationRoleHash, CapabilityGrantV1, CapabilityPermissionKindV1,
    CapabilityPermissionV1, CapabilityPermissionsV1, ContractBundleHash, ContractLineage,
    ContractVersion, EntityFieldVisibilityV1, Environment, PartitionScopeV1, QueryModuleHash,
    QueryOperationName, ReactiveModuleHash, TenantId, TenantScope, hash_application_role,
};

use crate::{ApplicationManifest, ManifestRole, ManifestTenantScope, QueryModule};

const ROLE_MAGIC: &[u8] = b"RIFFDB-APPLICATION-ROLE\0";
const ROLE_FORMAT_VERSION_V1: u32 = 1;
const ROLE_FORMAT_VERSION_V2: u32 = 2;
const ROLE_FORMAT_VERSION_V3: u32 = 3;
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
    row_policies: Vec<ApplicationRolePolicy>,
    principal_fact_schemas: Vec<ApplicationRoleFactSchema>,
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
    let (selected_policies, row_policies, principal_fact_schemas) =
        compile_role_policies(manifest, role, contract)?;

    let lineage = contract.lineage().clone();
    // ADR-0056 makes contract-description access a compiler-derived part of
    // every symbolic application role. This is required for an application
    // driver to prove the exact active lineage/version/bundle before exposing
    // its generated operation catalog; callers never assemble this grant.
    let mut permissions = Vec::with_capacity(role.queries().len() + role.commands().len() + 1);
    permissions.push(CapabilityPermissionV1::Unparameterized(
        CapabilityPermissionKindV1::ReadContract,
    ));
    let mut fields_by_entity = BTreeMap::<_, BTreeSet<_>>::new();
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
        maximum_rows = maximum_rows.max(query.plan().cost().scanned_index_rows());
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
        // WP-570 freezes the symbolic policy proof and identities. Until
        // WP-572 installs the shared transaction-current evaluator, granting
        // the ordinary operation permission would allow the existing runtime
        // to execute without the policy. Withhold it so partial rollout is a
        // closed authorization failure, never an application-side check.
        if !requires_row_policy {
            permissions.push(CapabilityPermissionV1::ExecuteNamedQuery(
                lineage.clone(),
                module.identity(),
                operation_name,
            ));
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
            .find(|command| command.name() == command_name)
            .ok_or_else(|| ApplicationRoleError::new(ApplicationRoleErrorKind::UnknownOperation))?;
        let mut requires_row_policy = false;
        for binding in command.bindings() {
            let operation = match binding.mode() {
                BindingMode::Read => RowPolicyOperationV1::Read,
                BindingMode::Mutate => RowPolicyOperationV1::Update,
                BindingMode::Create => RowPolicyOperationV1::Create,
                BindingMode::Delete => RowPolicyOperationV1::Delete,
            };
            require_policy_operation(
                contract,
                &selected_policies,
                binding.entity_type(),
                operation,
            )?;
            requires_row_policy |= contract
                .row_policies()
                .policies()
                .iter()
                .any(|policy| policy.entity() == binding.entity_type());
        }
        if !requires_row_policy {
            permissions.push(CapabilityPermissionV1::InvokeCommand(
                lineage.clone(),
                command.command_id(),
            ));
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

    let permissions = CapabilityPermissionsV1::new(permissions)
        .map_err(|_| ApplicationRoleError::new(ApplicationRoleErrorKind::RequirementLimit))?;
    let field_visibility = fields_by_entity
        .into_iter()
        .filter_map(|(entity, fields)| {
            (!fields.is_empty()).then_some(EntityFieldVisibilityV1::new(
                lineage.clone(),
                entity,
                fields.into_iter().collect(),
            ))
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
        &base_grant,
    )?;
    let identity = hash_application_role(&canonical);
    let mut final_permissions = base_grant.permissions().as_slice().to_vec();
    final_permissions.push(CapabilityPermissionV1::ApplicationRoleIdentity(identity));
    let grant = CapabilityGrantV1::new(
        tenant_scope.clone(),
        PartitionScopeV1::All,
        CapabilityPermissionsV1::new(final_permissions)
            .map_err(|_| ApplicationRoleError::new(ApplicationRoleErrorKind::RequirementLimit))?,
        base_grant.field_visibility().to_vec(),
        base_grant.max_scan_rows(),
        Vec::new(),
    )
    .map_err(|_| ApplicationRoleError::new(ApplicationRoleErrorKind::RequirementLimit))?;
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
        row_policies,
        principal_fact_schemas,
        identity,
        grant,
    })
}

type SelectedPolicyMap<'a> = BTreeMap<riffdb_types::EntityTypeId, &'a RowPolicyPlanV1>;

fn compile_role_policies<'a>(
    manifest: &ApplicationManifest,
    role: &ManifestRole,
    contract: &'a ContractBundle,
) -> Result<
    (
        SelectedPolicyMap<'a>,
        Vec<ApplicationRolePolicy>,
        Vec<ApplicationRoleFactSchema>,
    ),
    ApplicationRoleError,
> {
    let catalog = contract.row_policies();
    if manifest.schema() != crate::APPLICATION_MANIFEST_SCHEMA_V4 {
        if !catalog.is_empty() {
            return Err(ApplicationRoleError::new(
                ApplicationRoleErrorKind::ContractMismatch,
            ));
        }
        return Ok((BTreeMap::new(), Vec::new(), Vec::new()));
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
    let principal_fact_schemas = fact_names
        .into_iter()
        .map(|name| {
            let fact = facts_by_name.get(name.as_str()).ok_or_else(|| {
                ApplicationRoleError::new(ApplicationRoleErrorKind::ContractMismatch)
            })?;
            Ok(ApplicationRoleFactSchema {
                name,
                value_type: render_fact_type(fact, contract)?,
            })
        })
        .collect::<Result<Vec<_>, ApplicationRoleError>>()?;
    Ok((selected, descriptions, principal_fact_schemas))
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
    grant: &CapabilityGrantV1,
) -> Result<Vec<u8>, ApplicationRoleError> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(ROLE_MAGIC);
    bytes.extend_from_slice(
        &(if manifest.schema() == crate::APPLICATION_MANIFEST_SCHEMA_V4 {
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
    for visibility in grant.field_visibility() {
        bytes.extend_from_slice(&visibility.entity_type().to_be_bytes());
        for field in visibility.fields() {
            bytes.extend_from_slice(&field.to_be_bytes());
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
