use std::collections::BTreeSet;
use std::error::Error;
use std::fmt;

use riffdb_types::{
    AdapterConformanceManifestHash, ApplicationInstallationPlanHash, ApplicationLockHash,
    ApplicationManifestHash, ApplicationRoleHash, ApplicationSourceHash, CapabilityId,
    ContractBundleHash, ContractLineage, ContractVersion, DatabaseAlias, Environment,
    GeneratedArtifactHash, MigrationBundleHash, hash_application_installation_plan,
};
use serde::{Deserialize, Serialize};

/// Canonical schema version for exact application installation plans.
pub const APPLICATION_INSTALLATION_PLAN_SCHEMA_V1: &str = "riffdb.application-installation-plan/v1";
/// Maximum canonical bytes in one installation plan.
pub const MAX_INSTALLATION_PLAN_BYTES: usize = 4 * 1_024 * 1_024;
/// Maximum exact artifacts in one plan.
pub const MAX_INSTALLATION_ARTIFACTS: usize = 256;
/// Maximum symbolic application roles in one plan.
pub const MAX_INSTALLATION_ROLES: usize = 128;
/// Maximum credential destinations in one plan.
pub const MAX_CREDENTIAL_DESTINATIONS: usize = 256;
/// Maximum bounded seed batches in one plan.
pub const MAX_INSTALLATION_SEEDS: usize = 256;
/// Maximum ordinary commands represented by one seed batch.
pub const MAX_SEED_BATCH_ITEMS: u64 = 1_000_000;
/// Maximum symbolic operations in one application role.
pub const MAX_ROLE_OPERATIONS: usize = 1_024;
/// Maximum bytes in one installation-local symbolic name.
pub const MAX_INSTALLATION_SYMBOL_BYTES: usize = 256;

/// A bounded, non-secret symbol retained in plans and diagnostics.
#[derive(Clone, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct InstallationSymbol(String);

impl InstallationSymbol {
    /// Validates one exact source-like symbol without normalization.
    pub fn new(value: impl Into<String>) -> Result<Self, InstallationPlanError> {
        let value = value.into();
        if value.is_empty() || value.len() > MAX_INSTALLATION_SYMBOL_BYTES {
            return Err(InstallationPlanError::new(
                InstallationPlanErrorKind::LimitExceeded,
            ));
        }
        if let Some(index) = value.bytes().enumerate().find_map(|(index, byte)| {
            (!(byte == b'_'
                || byte == b'-'
                || byte == b'.'
                || byte.is_ascii_alphabetic()
                || index > 0 && byte.is_ascii_digit()))
            .then_some(index)
        }) {
            let _ = index;
            return Err(InstallationPlanError::new(
                InstallationPlanErrorKind::InvalidIdentifier,
            ));
        }
        Ok(Self(value))
    }

    /// Borrows the exact symbol.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for InstallationSymbol {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_tuple("InstallationSymbol")
            .field(&self.0)
            .finish()
    }
}

/// Exact selected database, environment, and application lineage.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InstallationTarget {
    database: DatabaseAlias,
    environment: Environment,
    lineage: ContractLineage,
}

impl InstallationTarget {
    /// Creates an exact installation authority target.
    #[must_use]
    pub const fn new(
        database: DatabaseAlias,
        environment: Environment,
        lineage: ContractLineage,
    ) -> Self {
        Self {
            database,
            environment,
            lineage,
        }
    }

    /// Selected canonical database alias.
    #[must_use]
    pub const fn database(&self) -> &DatabaseAlias {
        &self.database
    }

    /// Selected exact deployment environment.
    #[must_use]
    pub const fn environment(&self) -> &Environment {
        &self.environment
    }

    /// Selected application lineage.
    #[must_use]
    pub const fn lineage(&self) -> &ContractLineage {
        &self.lineage
    }
}

/// Candidate contract identity pinned by the exact lock.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct InstallationContract {
    version: ContractVersion,
    bundle_hash: ContractBundleHash,
}

impl InstallationContract {
    /// Binds one exact candidate version and bundle.
    #[must_use]
    pub const fn new(version: ContractVersion, bundle_hash: ContractBundleHash) -> Self {
        Self {
            version,
            bundle_hash,
        }
    }

    /// Exact candidate version.
    #[must_use]
    pub const fn version(self) -> ContractVersion {
        self.version
    }

    /// Exact candidate bundle hash.
    #[must_use]
    pub const fn bundle_hash(self) -> ContractBundleHash {
        self.bundle_hash
    }
}

/// Closed compiler-owned artifact class.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum InstallationArtifactKind {
    /// Canonical application manifest.
    Manifest,
    /// Canonical contract bundle.
    ContractBundle,
    /// Named-query module.
    QueryModule,
    /// Reactive module.
    ReactiveModule,
    /// Compiled migration bundle.
    MigrationBundle,
    /// Rust generated bindings.
    Rust,
    /// TypeScript generated bindings.
    TypeScript,
    /// Go generated bindings.
    Go,
    /// Python generated bindings.
    Python,
    /// Generated MCP catalog.
    Mcp,
    /// Bounded seed input.
    SeedInput,
    /// Adapter conformance manifest.
    AdapterManifest,
}

impl InstallationArtifactKind {
    pub(crate) const fn tag(self) -> &'static str {
        match self {
            Self::Manifest => "manifest",
            Self::ContractBundle => "contract_bundle",
            Self::QueryModule => "query_module",
            Self::ReactiveModule => "reactive_module",
            Self::MigrationBundle => "migration_bundle",
            Self::Rust => "rust",
            Self::TypeScript => "typescript",
            Self::Go => "go",
            Self::Python => "python",
            Self::Mcp => "mcp",
            Self::SeedInput => "seed_input",
            Self::AdapterManifest => "adapter_manifest",
        }
    }

    pub(crate) fn parse(value: &str) -> Option<Self> {
        Some(match value {
            "manifest" => Self::Manifest,
            "contract_bundle" => Self::ContractBundle,
            "query_module" => Self::QueryModule,
            "reactive_module" => Self::ReactiveModule,
            "migration_bundle" => Self::MigrationBundle,
            "rust" => Self::Rust,
            "typescript" => Self::TypeScript,
            "go" => Self::Go,
            "python" => Self::Python,
            "mcp" => Self::Mcp,
            "seed_input" => Self::SeedInput,
            "adapter_manifest" => Self::AdapterManifest,
            _ => return None,
        })
    }
}

/// One exact compiler-produced artifact without a host filesystem path.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct InstallationArtifact {
    kind: InstallationArtifactKind,
    name: InstallationSymbol,
    content_hash: GeneratedArtifactHash,
}

impl InstallationArtifact {
    /// Creates one name-addressed exact artifact reference.
    #[must_use]
    pub const fn new(
        kind: InstallationArtifactKind,
        name: InstallationSymbol,
        content_hash: GeneratedArtifactHash,
    ) -> Self {
        Self {
            kind,
            name,
            content_hash,
        }
    }

    /// Closed artifact kind.
    #[must_use]
    pub const fn kind(&self) -> InstallationArtifactKind {
        self.kind
    }

    /// Symbolic artifact name, never a host path.
    #[must_use]
    pub const fn name(&self) -> &InstallationSymbol {
        &self.name
    }

    /// Exact generated content identity.
    #[must_use]
    pub const fn content_hash(&self) -> GeneratedArtifactHash {
        self.content_hash
    }
}

/// Closed symbolic operation category used only to compute authority diffs.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum RoleOperationKind {
    /// Named query.
    Query,
    /// Compiled command.
    Command,
    /// Durable event stream.
    EventStream,
    /// Live named-query watch.
    QueryWatch,
    /// Contextual agent subscription.
    AgentSubscription,
}

impl RoleOperationKind {
    pub(crate) const fn tag(self) -> &'static str {
        match self {
            Self::Query => "query",
            Self::Command => "command",
            Self::EventStream => "event_stream",
            Self::QueryWatch => "query_watch",
            Self::AgentSubscription => "agent_subscription",
        }
    }

    pub(crate) fn parse(value: &str) -> Option<Self> {
        Some(match value {
            "query" => Self::Query,
            "command" => Self::Command,
            "event_stream" => Self::EventStream,
            "query_watch" => Self::QueryWatch,
            "agent_subscription" => Self::AgentSubscription,
            _ => return None,
        })
    }
}

/// One symbolic application operation in an authority diff.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct RoleOperation {
    kind: RoleOperationKind,
    name: InstallationSymbol,
}

impl RoleOperation {
    /// Creates one symbolic operation.
    #[must_use]
    pub const fn new(kind: RoleOperationKind, name: InstallationSymbol) -> Self {
        Self { kind, name }
    }

    /// Operation category.
    #[must_use]
    pub const fn kind(&self) -> RoleOperationKind {
        self.kind
    }

    /// Exact operation symbol.
    #[must_use]
    pub const fn name(&self) -> &InstallationSymbol {
        &self.name
    }
}

/// Exact approval for only the additions in one existing role.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RoleWideningApproval {
    expected_previous_role: ApplicationRoleHash,
    additions: Vec<RoleOperation>,
}

impl RoleWideningApproval {
    /// Creates an approval whose exact additions are checked during compilation.
    pub fn new(
        expected_previous_role: ApplicationRoleHash,
        mut additions: Vec<RoleOperation>,
    ) -> Result<Self, InstallationPlanError> {
        sort_unique(&mut additions)?;
        if additions.is_empty() || additions.len() > MAX_ROLE_OPERATIONS {
            return Err(InstallationPlanError::new(
                InstallationPlanErrorKind::InvalidApproval,
            ));
        }
        Ok(Self {
            expected_previous_role,
            additions,
        })
    }

    /// Exact predecessor role identity being widened.
    #[must_use]
    pub const fn expected_previous_role(&self) -> ApplicationRoleHash {
        self.expected_previous_role
    }

    /// Exact symbolic additions approved by the operator.
    #[must_use]
    pub fn additions(&self) -> &[RoleOperation] {
        &self.additions
    }
}

/// One desired exact role plus enough predecessor state to compute its authority diff.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InstallationRole {
    name: InstallationSymbol,
    role_hash: ApplicationRoleHash,
    previous_role_hash: Option<ApplicationRoleHash>,
    desired_operations: Vec<RoleOperation>,
    previous_operations: Vec<RoleOperation>,
    widening_approval: Option<RoleWideningApproval>,
}

impl InstallationRole {
    /// Creates one exact initial or successor role.
    pub fn new(
        name: InstallationSymbol,
        role_hash: ApplicationRoleHash,
        previous_role_hash: Option<ApplicationRoleHash>,
        mut desired_operations: Vec<RoleOperation>,
        mut previous_operations: Vec<RoleOperation>,
        widening_approval: Option<RoleWideningApproval>,
    ) -> Result<Self, InstallationPlanError> {
        sort_unique(&mut desired_operations)?;
        sort_unique(&mut previous_operations)?;
        if desired_operations.is_empty()
            || desired_operations.len() > MAX_ROLE_OPERATIONS
            || previous_operations.len() > MAX_ROLE_OPERATIONS
            || (previous_role_hash.is_none() && !previous_operations.is_empty())
        {
            return Err(InstallationPlanError::new(
                InstallationPlanErrorKind::InvalidShape,
            ));
        }
        let additions = desired_operations
            .iter()
            .filter(|operation| previous_operations.binary_search(operation).is_err())
            .cloned()
            .collect::<Vec<_>>();
        match (
            previous_role_hash,
            additions.is_empty(),
            widening_approval.as_ref(),
        ) {
            (None, _, None) | (Some(_), true, None) => {}
            (Some(previous), false, Some(approval))
                if approval.expected_previous_role == previous
                    && approval.additions == additions => {}
            (Some(_), false, None) => {
                return Err(InstallationPlanError::new(
                    InstallationPlanErrorKind::ApprovalRequired,
                ));
            }
            _ => {
                return Err(InstallationPlanError::new(
                    InstallationPlanErrorKind::InvalidApproval,
                ));
            }
        }
        Ok(Self {
            name,
            role_hash,
            previous_role_hash,
            desired_operations,
            previous_operations,
            widening_approval,
        })
    }

    /// Symbolic role name.
    #[must_use]
    pub const fn name(&self) -> &InstallationSymbol {
        &self.name
    }

    /// Exact desired role identity.
    #[must_use]
    pub const fn role_hash(&self) -> ApplicationRoleHash {
        self.role_hash
    }

    /// Exact predecessor identity, when reconciling an existing role.
    #[must_use]
    pub const fn previous_role_hash(&self) -> Option<ApplicationRoleHash> {
        self.previous_role_hash
    }

    /// Desired symbolic operation surface.
    #[must_use]
    pub fn desired_operations(&self) -> &[RoleOperation] {
        &self.desired_operations
    }

    /// Existing symbolic operation surface used to compute the diff.
    #[must_use]
    pub fn previous_operations(&self) -> &[RoleOperation] {
        &self.previous_operations
    }

    /// Exact role-widening approval, present only when additions exist.
    #[must_use]
    pub const fn widening_approval(&self) -> Option<&RoleWideningApproval> {
        self.widening_approval.as_ref()
    }
}

/// One credential destination with exact no-overwrite semantics.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CredentialDestination {
    name: InstallationSymbol,
    role: InstallationSymbol,
    expected_current: Option<CapabilityId>,
    successor: CapabilityId,
}

impl CredentialDestination {
    /// Creates one empty-slot install or exact-predecessor replacement.
    pub fn new(
        name: InstallationSymbol,
        role: InstallationSymbol,
        expected_current: Option<CapabilityId>,
        successor: CapabilityId,
    ) -> Result<Self, InstallationPlanError> {
        if expected_current == Some(successor) {
            return Err(InstallationPlanError::new(
                InstallationPlanErrorKind::CredentialConflict,
            ));
        }
        Ok(Self {
            name,
            role,
            expected_current,
            successor,
        })
    }

    /// Symbolic destination name; this is never a host path.
    #[must_use]
    pub const fn name(&self) -> &InstallationSymbol {
        &self.name
    }

    /// Symbolic role whose credential is installed.
    #[must_use]
    pub const fn role(&self) -> &InstallationSymbol {
        &self.role
    }

    /// Exact current capability that may be replaced, or `None` for must-be-empty.
    #[must_use]
    pub const fn expected_current(&self) -> Option<CapabilityId> {
        self.expected_current
    }

    /// Caller-stable successor capability identity; credential bytes are never retained.
    #[must_use]
    pub const fn successor(&self) -> CapabilityId {
        self.successor
    }
}

/// Only backup posture admitted for an installation-driven migration.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MigrationBackupPolicy {
    /// Create and verify the operation-owned backup before staging.
    RequiredVerified,
}

/// Only downtime posture admitted by the current offline migration engine.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MigrationDowntimeClass {
    /// Reject application work for the complete migration operation.
    OfflineExclusive,
}

/// Exact, explicitly confirmed migration gate.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct InstallationMigration {
    parent_version: ContractVersion,
    parent_bundle_hash: ContractBundleHash,
    successor_version: ContractVersion,
    successor_bundle_hash: ContractBundleHash,
    migration_hash: MigrationBundleHash,
    confirmation: MigrationBundleHash,
    backup_policy: MigrationBackupPolicy,
    downtime: MigrationDowntimeClass,
}

impl InstallationMigration {
    /// Creates the only safe migration shape admitted by an installation plan.
    pub fn new(
        parent_version: ContractVersion,
        parent_bundle_hash: ContractBundleHash,
        successor_version: ContractVersion,
        successor_bundle_hash: ContractBundleHash,
        migration_hash: MigrationBundleHash,
        confirmation: MigrationBundleHash,
    ) -> Result<Self, InstallationPlanError> {
        if parent_version >= successor_version || migration_hash != confirmation {
            return Err(InstallationPlanError::new(
                InstallationPlanErrorKind::UnsafeMigration,
            ));
        }
        Ok(Self {
            parent_version,
            parent_bundle_hash,
            successor_version,
            successor_bundle_hash,
            migration_hash,
            confirmation,
            backup_policy: MigrationBackupPolicy::RequiredVerified,
            downtime: MigrationDowntimeClass::OfflineExclusive,
        })
    }

    /// Exact predecessor version.
    #[must_use]
    pub const fn parent_version(self) -> ContractVersion {
        self.parent_version
    }

    /// Exact predecessor bundle.
    #[must_use]
    pub const fn parent_bundle_hash(self) -> ContractBundleHash {
        self.parent_bundle_hash
    }

    /// Exact successor version.
    #[must_use]
    pub const fn successor_version(self) -> ContractVersion {
        self.successor_version
    }

    /// Exact successor bundle.
    #[must_use]
    pub const fn successor_bundle_hash(self) -> ContractBundleHash {
        self.successor_bundle_hash
    }

    /// Exact compiled migration identity.
    #[must_use]
    pub const fn migration_hash(self) -> MigrationBundleHash {
        self.migration_hash
    }

    /// Exact caller confirmation; always equal to the migration hash.
    #[must_use]
    pub const fn confirmation(self) -> MigrationBundleHash {
        self.confirmation
    }

    /// Required backup policy.
    #[must_use]
    pub const fn backup_policy(self) -> MigrationBackupPolicy {
        self.backup_policy
    }

    /// Required downtime class.
    #[must_use]
    pub const fn downtime(self) -> MigrationDowntimeClass {
        self.downtime
    }
}

/// First-class stable driver targets that must prove the installed identity.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum InstallationDriver {
    /// First-party Rust client.
    Rust,
    /// First-party TypeScript client.
    TypeScript,
    /// First-party Go client.
    Go,
    /// First-party Python client.
    Python,
}

impl InstallationDriver {
    pub(crate) const fn tag(self) -> &'static str {
        match self {
            Self::Rust => "rust",
            Self::TypeScript => "typescript",
            Self::Go => "go",
            Self::Python => "python",
        }
    }

    pub(crate) fn parse(value: &str) -> Option<Self> {
        Some(match value {
            "rust" => Self::Rust,
            "typescript" => Self::TypeScript,
            "go" => Self::Go,
            "python" => Self::Python,
            _ => return None,
        })
    }
}

/// Closed alpha application feature catalog consumed by installation planning.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum InstallationFeature {
    /// Authenticated remote TLS transport.
    RemoteTls,
    /// Compiler-visible bounded bulk commands.
    BulkCommands,
    /// Bounded operational RiffQL.
    OperationalQueries,
    /// Fenced workflow concurrency.
    WorkflowConcurrency,
    /// Principal-aware row policies.
    RowPolicies,
    /// Programmatic exact installation campaigns.
    InstallationCampaigns,
    /// Public export and compatibility lifecycle.
    DataLifecycle,
}

impl InstallationFeature {
    /// Complete closed alpha feature catalog in canonical order.
    pub const ALL: [Self; 7] = [
        Self::RemoteTls,
        Self::BulkCommands,
        Self::OperationalQueries,
        Self::WorkflowConcurrency,
        Self::RowPolicies,
        Self::InstallationCampaigns,
        Self::DataLifecycle,
    ];

    pub(crate) const fn tag(self) -> &'static str {
        match self {
            Self::RemoteTls => "remote_tls",
            Self::BulkCommands => "bulk_commands",
            Self::OperationalQueries => "operational_queries",
            Self::WorkflowConcurrency => "workflow_concurrency",
            Self::RowPolicies => "row_policies",
            Self::InstallationCampaigns => "installation_campaigns",
            Self::DataLifecycle => "data_lifecycle",
        }
    }

    pub(crate) fn parse(value: &str) -> Option<Self> {
        Some(match value {
            "remote_tls" => Self::RemoteTls,
            "bulk_commands" => Self::BulkCommands,
            "operational_queries" => Self::OperationalQueries,
            "workflow_concurrency" => Self::WorkflowConcurrency,
            "row_policies" => Self::RowPolicies,
            "installation_campaigns" => Self::InstallationCampaigns,
            "data_lifecycle" => Self::DataLifecycle,
            _ => return None,
        })
    }
}

/// One bounded seed batch whose items remain ordinary compiled commands.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct InstallationSeed {
    name: InstallationSymbol,
    content_hash: GeneratedArtifactHash,
    item_count: u64,
}

impl InstallationSeed {
    /// Binds one seed artifact and its exact nonzero item count.
    pub fn new(
        name: InstallationSymbol,
        content_hash: GeneratedArtifactHash,
        item_count: u64,
    ) -> Result<Self, InstallationPlanError> {
        if item_count == 0 || item_count > MAX_SEED_BATCH_ITEMS {
            return Err(InstallationPlanError::new(
                InstallationPlanErrorKind::LimitExceeded,
            ));
        }
        Ok(Self {
            name,
            content_hash,
            item_count,
        })
    }

    /// Symbolic seed name.
    #[must_use]
    pub const fn name(&self) -> &InstallationSymbol {
        &self.name
    }

    /// Exact content identity without exposing seed values.
    #[must_use]
    pub const fn content_hash(&self) -> GeneratedArtifactHash {
        self.content_hash
    }

    /// Number of individually idempotent ordinary command items.
    #[must_use]
    pub const fn item_count(&self) -> u64 {
        self.item_count
    }
}

/// Complete local input to deterministic installation-plan compilation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ApplicationInstallationPlanInput {
    /// Symbolic application name.
    pub application: InstallationSymbol,
    /// Exact author-source identity.
    pub source_hash: ApplicationSourceHash,
    /// Exact compiler-owned lock identity.
    pub lock_hash: ApplicationLockHash,
    /// Exact application-manifest identity.
    pub manifest_hash: ApplicationManifestHash,
    /// Exact installation authority target.
    pub target: InstallationTarget,
    /// Exact candidate contract.
    pub contract: InstallationContract,
    /// Complete exact generated artifact set.
    pub artifacts: Vec<InstallationArtifact>,
    /// Optional explicit offline migration gate.
    pub migration: Option<InstallationMigration>,
    /// Exact symbolic desired roles and predecessor authority observations.
    pub roles: Vec<InstallationRole>,
    /// Exact empty-or-predecessor-bound credential destinations.
    pub credential_destinations: Vec<CredentialDestination>,
    /// Public drivers that must prove the installed identity.
    pub drivers: Vec<InstallationDriver>,
    /// Bounded ordinary-command seed batches.
    pub seeds: Vec<InstallationSeed>,
    /// Closed features required by the application.
    pub required_features: Vec<InstallationFeature>,
    /// Optional exact adapter conformance manifest identity.
    pub adapter_manifest_hash: Option<AdapterConformanceManifestHash>,
}

/// One immutable content-addressed installation plan.
#[derive(Clone, Eq, PartialEq)]
pub struct ApplicationInstallationPlan {
    input: ApplicationInstallationPlanInput,
    identity: ApplicationInstallationPlanHash,
    canonical_bytes: Vec<u8>,
}

impl ApplicationInstallationPlan {
    /// Compiles a canonical plan without remote mutation.
    pub fn compile(
        mut input: ApplicationInstallationPlanInput,
    ) -> Result<Self, InstallationPlanError> {
        validate_and_sort(&mut input)?;
        let dto = PlanDto::from_input(&input);
        let mut canonical_bytes = serde_json::to_vec(&dto)
            .map_err(|_| InstallationPlanError::new(InstallationPlanErrorKind::InvalidEncoding))?;
        canonical_bytes.push(b'\n');
        if canonical_bytes.len() > MAX_INSTALLATION_PLAN_BYTES {
            return Err(InstallationPlanError::new(
                InstallationPlanErrorKind::LimitExceeded,
            ));
        }
        let identity = hash_application_installation_plan(&canonical_bytes);
        Ok(Self {
            input,
            identity,
            canonical_bytes,
        })
    }

    /// Strictly decodes canonical bytes and revalidates every semantic gate.
    pub fn decode_canonical(bytes: &[u8]) -> Result<Self, InstallationPlanError> {
        if bytes.is_empty() || bytes.len() > MAX_INSTALLATION_PLAN_BYTES {
            return Err(InstallationPlanError::new(
                InstallationPlanErrorKind::LimitExceeded,
            ));
        }
        let dto: PlanDto = serde_json::from_slice(bytes)
            .map_err(|_| InstallationPlanError::new(InstallationPlanErrorKind::InvalidEncoding))?;
        let plan = Self::compile(dto.into_input()?)?;
        if plan.canonical_bytes != bytes {
            return Err(InstallationPlanError::new(
                InstallationPlanErrorKind::NonCanonical,
            ));
        }
        Ok(plan)
    }

    /// Exact plan identity.
    #[must_use]
    pub const fn identity(&self) -> ApplicationInstallationPlanHash {
        self.identity
    }

    /// Complete validated input retained by the plan.
    #[must_use]
    pub const fn input(&self) -> &ApplicationInstallationPlanInput {
        &self.input
    }

    /// Canonical compatibility bytes.
    #[must_use]
    pub fn canonical_bytes(&self) -> &[u8] {
        &self.canonical_bytes
    }
}

impl fmt::Debug for ApplicationInstallationPlan {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ApplicationInstallationPlan")
            .field("identity", &self.identity)
            .field("application", &self.input.application)
            .field("target", &self.input.target)
            .field("artifacts", &self.input.artifacts.len())
            .field("roles", &self.input.roles.len())
            .field("credentials", &self.input.credential_destinations.len())
            .field("seeds", &self.input.seeds.len())
            .finish_non_exhaustive()
    }
}

/// Closed deterministic plan-compilation failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum InstallationPlanErrorKind {
    /// A bounded symbolic identifier is invalid.
    InvalidIdentifier,
    /// One hard input or encoded-size bound was exceeded.
    LimitExceeded,
    /// A supposedly unique symbolic key was duplicated.
    Duplicate,
    /// Exact source, lock, manifest, contract, or artifact identities disagree.
    IdentityMismatch,
    /// An existing role would gain authority without explicit exact approval.
    ApprovalRequired,
    /// A supplied authority approval is stale, broader, or otherwise inexact.
    InvalidApproval,
    /// Migration confirmation, ordering, backup, or downtime semantics are unsafe.
    UnsafeMigration,
    /// A credential replacement is ambiguous or would overwrite its predecessor.
    CredentialConflict,
    /// The decoded version is not supported.
    UnsupportedVersion,
    /// The submitted document is malformed.
    InvalidEncoding,
    /// The submitted document has valid meaning but noncanonical bytes.
    NonCanonical,
    /// The submitted values do not form a closed installation shape.
    InvalidShape,
}

/// Bounded, redaction-safe plan compilation failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct InstallationPlanError {
    kind: InstallationPlanErrorKind,
}

impl InstallationPlanError {
    const fn new(kind: InstallationPlanErrorKind) -> Self {
        Self { kind }
    }

    /// Stable error kind.
    #[must_use]
    pub const fn kind(self) -> InstallationPlanErrorKind {
        self.kind
    }

    pub(crate) const fn from_kind_for_diff(kind: InstallationPlanErrorKind) -> Self {
        Self::new(kind)
    }
}

impl fmt::Display for InstallationPlanError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self.kind {
            InstallationPlanErrorKind::InvalidIdentifier => {
                "application installation symbol is invalid"
            }
            InstallationPlanErrorKind::LimitExceeded => {
                "application installation input exceeds a hard bound"
            }
            InstallationPlanErrorKind::Duplicate => {
                "application installation input contains a duplicate symbolic key"
            }
            InstallationPlanErrorKind::IdentityMismatch => {
                "application installation exact identities disagree"
            }
            InstallationPlanErrorKind::ApprovalRequired => {
                "application role widening requires explicit exact approval"
            }
            InstallationPlanErrorKind::InvalidApproval => {
                "application role widening approval is stale or inexact"
            }
            InstallationPlanErrorKind::UnsafeMigration => {
                "application migration gate is absent, stale, or unsafe"
            }
            InstallationPlanErrorKind::CredentialConflict => {
                "application credential destination cannot be replaced safely"
            }
            InstallationPlanErrorKind::UnsupportedVersion => {
                "application installation plan version is unsupported"
            }
            InstallationPlanErrorKind::InvalidEncoding => {
                "application installation plan encoding is invalid"
            }
            InstallationPlanErrorKind::NonCanonical => {
                "application installation plan encoding is not canonical"
            }
            InstallationPlanErrorKind::InvalidShape => {
                "application installation plan shape is invalid"
            }
        })
    }
}

impl Error for InstallationPlanError {}

fn validate_and_sort(
    input: &mut ApplicationInstallationPlanInput,
) -> Result<(), InstallationPlanError> {
    if input.artifacts.is_empty()
        || input.artifacts.len() > MAX_INSTALLATION_ARTIFACTS
        || input.roles.is_empty()
        || input.roles.len() > MAX_INSTALLATION_ROLES
        || input.credential_destinations.len() > MAX_CREDENTIAL_DESTINATIONS
        || input.seeds.len() > MAX_INSTALLATION_SEEDS
    {
        return Err(InstallationPlanError::new(
            InstallationPlanErrorKind::LimitExceeded,
        ));
    }
    input.artifacts.sort();
    if input
        .artifacts
        .windows(2)
        .any(|pair| (pair[0].kind, &pair[0].name) == (pair[1].kind, &pair[1].name))
    {
        return Err(InstallationPlanError::new(
            InstallationPlanErrorKind::Duplicate,
        ));
    }
    if !input
        .artifacts
        .iter()
        .any(|artifact| artifact.kind == InstallationArtifactKind::Manifest)
        || !input
            .artifacts
            .iter()
            .any(|artifact| artifact.kind == InstallationArtifactKind::ContractBundle)
    {
        return Err(InstallationPlanError::new(
            InstallationPlanErrorKind::InvalidShape,
        ));
    }
    input
        .roles
        .sort_by(|left, right| left.name.cmp(&right.name));
    ensure_unique_by(input.roles.iter().map(|role| &role.name))?;
    input
        .credential_destinations
        .sort_by(|left, right| left.name.cmp(&right.name));
    ensure_unique_by(
        input
            .credential_destinations
            .iter()
            .map(|destination| &destination.name),
    )?;
    for destination in &input.credential_destinations {
        if input
            .roles
            .binary_search_by(|role| role.name.cmp(&destination.role))
            .is_err()
        {
            return Err(InstallationPlanError::new(
                InstallationPlanErrorKind::IdentityMismatch,
            ));
        }
    }
    input.seeds.sort();
    ensure_unique_by(input.seeds.iter().map(|seed| &seed.name))?;
    sort_unique(&mut input.drivers)?;
    sort_unique(&mut input.required_features)?;
    if input.drivers.is_empty()
        || !input
            .required_features
            .contains(&InstallationFeature::InstallationCampaigns)
    {
        return Err(InstallationPlanError::new(
            InstallationPlanErrorKind::InvalidShape,
        ));
    }
    if let Some(migration) = input.migration
        && (migration.successor_version != input.contract.version
            || migration.successor_bundle_hash != input.contract.bundle_hash
            || migration.migration_hash != migration.confirmation
            || !input
                .artifacts
                .iter()
                .any(|artifact| artifact.kind == InstallationArtifactKind::MigrationBundle))
    {
        return Err(InstallationPlanError::new(
            InstallationPlanErrorKind::IdentityMismatch,
        ));
    }
    let adapter_artifact = input
        .artifacts
        .iter()
        .find(|artifact| artifact.kind == InstallationArtifactKind::AdapterManifest);
    if input.adapter_manifest_hash.is_some() != adapter_artifact.is_some()
        || input.adapter_manifest_hash.is_some_and(|manifest_hash| {
            adapter_artifact
                .is_none_or(|artifact| artifact.content_hash.as_bytes() != manifest_hash.as_bytes())
        })
    {
        return Err(InstallationPlanError::new(
            InstallationPlanErrorKind::IdentityMismatch,
        ));
    }
    Ok(())
}

fn sort_unique<T: Ord>(values: &mut [T]) -> Result<(), InstallationPlanError> {
    values.sort();
    if values.windows(2).any(|pair| pair[0] == pair[1]) {
        return Err(InstallationPlanError::new(
            InstallationPlanErrorKind::Duplicate,
        ));
    }
    Ok(())
}

fn ensure_unique_by<'a, T: Ord + 'a>(
    values: impl Iterator<Item = &'a T>,
) -> Result<(), InstallationPlanError> {
    let mut seen = BTreeSet::new();
    if values.into_iter().any(|value| !seen.insert(value)) {
        return Err(InstallationPlanError::new(
            InstallationPlanErrorKind::Duplicate,
        ));
    }
    Ok(())
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PlanDto {
    schema: String,
    application: String,
    source_hash: String,
    lock_hash: String,
    manifest_hash: String,
    target: TargetDto,
    contract: ContractDto,
    artifacts: Vec<ArtifactDto>,
    migration: Option<MigrationDto>,
    roles: Vec<RoleDto>,
    credential_destinations: Vec<CredentialDto>,
    drivers: Vec<String>,
    seeds: Vec<SeedDto>,
    required_features: Vec<String>,
    adapter_manifest_hash: Option<String>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct TargetDto {
    database: String,
    environment: String,
    lineage: String,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ContractDto {
    version: u64,
    bundle_hash: String,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ArtifactDto {
    kind: String,
    name: String,
    content_hash: String,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct OperationDto {
    kind: String,
    name: String,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ApprovalDto {
    expected_previous_role: String,
    additions: Vec<OperationDto>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RoleDto {
    name: String,
    role_hash: String,
    previous_role_hash: Option<String>,
    desired_operations: Vec<OperationDto>,
    previous_operations: Vec<OperationDto>,
    widening_approval: Option<ApprovalDto>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct CredentialDto {
    name: String,
    role: String,
    expected_current: Option<String>,
    successor: String,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct MigrationDto {
    parent_version: u64,
    parent_bundle_hash: String,
    successor_version: u64,
    successor_bundle_hash: String,
    migration_hash: String,
    confirmation: String,
    backup_policy: String,
    downtime: String,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct SeedDto {
    name: String,
    content_hash: String,
    item_count: u64,
}

impl PlanDto {
    fn from_input(input: &ApplicationInstallationPlanInput) -> Self {
        Self {
            schema: APPLICATION_INSTALLATION_PLAN_SCHEMA_V1.to_owned(),
            application: input.application.as_str().to_owned(),
            source_hash: hex32(input.source_hash.as_bytes()),
            lock_hash: hex32(input.lock_hash.as_bytes()),
            manifest_hash: hex32(input.manifest_hash.as_bytes()),
            target: TargetDto {
                database: input.target.database.as_str().to_owned(),
                environment: input.target.environment.as_str().to_owned(),
                lineage: input.target.lineage.as_str().to_owned(),
            },
            contract: ContractDto {
                version: input.contract.version.get(),
                bundle_hash: hex32(input.contract.bundle_hash.as_bytes()),
            },
            artifacts: input
                .artifacts
                .iter()
                .map(|artifact| ArtifactDto {
                    kind: artifact.kind.tag().to_owned(),
                    name: artifact.name.as_str().to_owned(),
                    content_hash: hex32(artifact.content_hash.as_bytes()),
                })
                .collect(),
            migration: input.migration.map(|migration| MigrationDto {
                parent_version: migration.parent_version.get(),
                parent_bundle_hash: hex32(migration.parent_bundle_hash.as_bytes()),
                successor_version: migration.successor_version.get(),
                successor_bundle_hash: hex32(migration.successor_bundle_hash.as_bytes()),
                migration_hash: hex32(migration.migration_hash.as_bytes()),
                confirmation: hex32(migration.confirmation.as_bytes()),
                backup_policy: "required_verified".to_owned(),
                downtime: "offline_exclusive".to_owned(),
            }),
            roles: input.roles.iter().map(RoleDto::from_role).collect(),
            credential_destinations: input
                .credential_destinations
                .iter()
                .map(|destination| CredentialDto {
                    name: destination.name.as_str().to_owned(),
                    role: destination.role.as_str().to_owned(),
                    expected_current: destination
                        .expected_current
                        .map(|value| hex16(value.as_bytes())),
                    successor: hex16(destination.successor.as_bytes()),
                })
                .collect(),
            drivers: input
                .drivers
                .iter()
                .map(|driver| driver.tag().to_owned())
                .collect(),
            seeds: input
                .seeds
                .iter()
                .map(|seed| SeedDto {
                    name: seed.name.as_str().to_owned(),
                    content_hash: hex32(seed.content_hash.as_bytes()),
                    item_count: seed.item_count,
                })
                .collect(),
            required_features: input
                .required_features
                .iter()
                .map(|feature| feature.tag().to_owned())
                .collect(),
            adapter_manifest_hash: input
                .adapter_manifest_hash
                .map(|value| hex32(value.as_bytes())),
        }
    }

    fn into_input(self) -> Result<ApplicationInstallationPlanInput, InstallationPlanError> {
        if self.schema != APPLICATION_INSTALLATION_PLAN_SCHEMA_V1 {
            return Err(InstallationPlanError::new(
                InstallationPlanErrorKind::UnsupportedVersion,
            ));
        }
        Ok(ApplicationInstallationPlanInput {
            application: InstallationSymbol::new(self.application)?,
            source_hash: ApplicationSourceHash::from_bytes(parse_hex32(&self.source_hash)?),
            lock_hash: ApplicationLockHash::from_bytes(parse_hex32(&self.lock_hash)?),
            manifest_hash: ApplicationManifestHash::from_bytes(parse_hex32(&self.manifest_hash)?),
            target: InstallationTarget::new(
                DatabaseAlias::new(self.target.database).map_err(|_| {
                    InstallationPlanError::new(InstallationPlanErrorKind::InvalidIdentifier)
                })?,
                Environment::new(self.target.environment).map_err(|_| {
                    InstallationPlanError::new(InstallationPlanErrorKind::InvalidIdentifier)
                })?,
                ContractLineage::new(self.target.lineage).map_err(|_| {
                    InstallationPlanError::new(InstallationPlanErrorKind::InvalidIdentifier)
                })?,
            ),
            contract: InstallationContract::new(
                ContractVersion::new(self.contract.version).ok_or_else(|| {
                    InstallationPlanError::new(InstallationPlanErrorKind::InvalidShape)
                })?,
                ContractBundleHash::from_bytes(parse_hex32(&self.contract.bundle_hash)?),
            ),
            artifacts: self
                .artifacts
                .into_iter()
                .map(ArtifactDto::into_artifact)
                .collect::<Result<Vec<_>, _>>()?,
            migration: self
                .migration
                .map(MigrationDto::into_migration)
                .transpose()?,
            roles: self
                .roles
                .into_iter()
                .map(RoleDto::into_role)
                .collect::<Result<Vec<_>, _>>()?,
            credential_destinations: self
                .credential_destinations
                .into_iter()
                .map(CredentialDto::into_destination)
                .collect::<Result<Vec<_>, _>>()?,
            drivers: self
                .drivers
                .into_iter()
                .map(|driver| {
                    InstallationDriver::parse(&driver).ok_or_else(|| {
                        InstallationPlanError::new(InstallationPlanErrorKind::InvalidShape)
                    })
                })
                .collect::<Result<Vec<_>, _>>()?,
            seeds: self
                .seeds
                .into_iter()
                .map(SeedDto::into_seed)
                .collect::<Result<Vec<_>, _>>()?,
            required_features: self
                .required_features
                .into_iter()
                .map(|feature| {
                    InstallationFeature::parse(&feature).ok_or_else(|| {
                        InstallationPlanError::new(InstallationPlanErrorKind::InvalidShape)
                    })
                })
                .collect::<Result<Vec<_>, _>>()?,
            adapter_manifest_hash: self
                .adapter_manifest_hash
                .map(|hash| parse_hex32(&hash).map(AdapterConformanceManifestHash::from_bytes))
                .transpose()?,
        })
    }
}

impl ArtifactDto {
    fn into_artifact(self) -> Result<InstallationArtifact, InstallationPlanError> {
        Ok(InstallationArtifact::new(
            InstallationArtifactKind::parse(&self.kind).ok_or_else(|| {
                InstallationPlanError::new(InstallationPlanErrorKind::InvalidShape)
            })?,
            InstallationSymbol::new(self.name)?,
            GeneratedArtifactHash::from_bytes(parse_hex32(&self.content_hash)?),
        ))
    }
}

impl OperationDto {
    fn from_operation(operation: &RoleOperation) -> Self {
        Self {
            kind: operation.kind.tag().to_owned(),
            name: operation.name.as_str().to_owned(),
        }
    }

    fn into_operation(self) -> Result<RoleOperation, InstallationPlanError> {
        Ok(RoleOperation::new(
            RoleOperationKind::parse(&self.kind).ok_or_else(|| {
                InstallationPlanError::new(InstallationPlanErrorKind::InvalidShape)
            })?,
            InstallationSymbol::new(self.name)?,
        ))
    }
}

impl RoleDto {
    fn from_role(role: &InstallationRole) -> Self {
        Self {
            name: role.name.as_str().to_owned(),
            role_hash: hex32(role.role_hash.as_bytes()),
            previous_role_hash: role.previous_role_hash.map(|hash| hex32(hash.as_bytes())),
            desired_operations: role
                .desired_operations
                .iter()
                .map(OperationDto::from_operation)
                .collect(),
            previous_operations: role
                .previous_operations
                .iter()
                .map(OperationDto::from_operation)
                .collect(),
            widening_approval: role.widening_approval.as_ref().map(|approval| ApprovalDto {
                expected_previous_role: hex32(approval.expected_previous_role.as_bytes()),
                additions: approval
                    .additions
                    .iter()
                    .map(OperationDto::from_operation)
                    .collect(),
            }),
        }
    }

    fn into_role(self) -> Result<InstallationRole, InstallationPlanError> {
        let approval = self
            .widening_approval
            .map(|approval| {
                RoleWideningApproval::new(
                    ApplicationRoleHash::from_bytes(parse_hex32(&approval.expected_previous_role)?),
                    approval
                        .additions
                        .into_iter()
                        .map(OperationDto::into_operation)
                        .collect::<Result<Vec<_>, _>>()?,
                )
            })
            .transpose()?;
        InstallationRole::new(
            InstallationSymbol::new(self.name)?,
            ApplicationRoleHash::from_bytes(parse_hex32(&self.role_hash)?),
            self.previous_role_hash
                .map(|hash| parse_hex32(&hash).map(ApplicationRoleHash::from_bytes))
                .transpose()?,
            self.desired_operations
                .into_iter()
                .map(OperationDto::into_operation)
                .collect::<Result<Vec<_>, _>>()?,
            self.previous_operations
                .into_iter()
                .map(OperationDto::into_operation)
                .collect::<Result<Vec<_>, _>>()?,
            approval,
        )
    }
}

impl CredentialDto {
    fn into_destination(self) -> Result<CredentialDestination, InstallationPlanError> {
        CredentialDestination::new(
            InstallationSymbol::new(self.name)?,
            InstallationSymbol::new(self.role)?,
            self.expected_current
                .map(|value| parse_capability_id(&value))
                .transpose()?,
            parse_capability_id(&self.successor)?,
        )
    }
}

impl MigrationDto {
    fn into_migration(self) -> Result<InstallationMigration, InstallationPlanError> {
        if self.backup_policy != "required_verified" || self.downtime != "offline_exclusive" {
            return Err(InstallationPlanError::new(
                InstallationPlanErrorKind::UnsafeMigration,
            ));
        }
        InstallationMigration::new(
            ContractVersion::new(self.parent_version).ok_or_else(|| {
                InstallationPlanError::new(InstallationPlanErrorKind::InvalidShape)
            })?,
            ContractBundleHash::from_bytes(parse_hex32(&self.parent_bundle_hash)?),
            ContractVersion::new(self.successor_version).ok_or_else(|| {
                InstallationPlanError::new(InstallationPlanErrorKind::InvalidShape)
            })?,
            ContractBundleHash::from_bytes(parse_hex32(&self.successor_bundle_hash)?),
            MigrationBundleHash::from_bytes(parse_hex32(&self.migration_hash)?),
            MigrationBundleHash::from_bytes(parse_hex32(&self.confirmation)?),
        )
    }
}

impl SeedDto {
    fn into_seed(self) -> Result<InstallationSeed, InstallationPlanError> {
        InstallationSeed::new(
            InstallationSymbol::new(self.name)?,
            GeneratedArtifactHash::from_bytes(parse_hex32(&self.content_hash)?),
            self.item_count,
        )
    }
}

pub(crate) fn hex32(bytes: &[u8; 32]) -> String {
    hex(bytes)
}

pub(crate) fn hex16(bytes: &[u8; 16]) -> String {
    hex(bytes)
}

fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(char::from(DIGITS[usize::from(byte >> 4)]));
        output.push(char::from(DIGITS[usize::from(byte & 0x0f)]));
    }
    output
}

pub(crate) fn parse_hex32(value: &str) -> Result<[u8; 32], InstallationPlanError> {
    parse_hex(value)
}

pub(crate) fn parse_hex16(value: &str) -> Result<[u8; 16], InstallationPlanError> {
    parse_hex(value)
}

fn parse_hex<const N: usize>(value: &str) -> Result<[u8; N], InstallationPlanError> {
    if value.len() != N * 2 {
        return Err(InstallationPlanError::new(
            InstallationPlanErrorKind::InvalidEncoding,
        ));
    }
    let mut bytes = [0_u8; N];
    for (index, pair) in value.as_bytes().chunks_exact(2).enumerate() {
        bytes[index] = (hex_nibble(pair[0])? << 4) | hex_nibble(pair[1])?;
    }
    Ok(bytes)
}

fn hex_nibble(byte: u8) -> Result<u8, InstallationPlanError> {
    match byte {
        b'0'..=b'9' => Ok(byte - b'0'),
        b'a'..=b'f' => Ok(byte - b'a' + 10),
        _ => Err(InstallationPlanError::new(
            InstallationPlanErrorKind::InvalidEncoding,
        )),
    }
}

fn parse_capability_id(value: &str) -> Result<CapabilityId, InstallationPlanError> {
    CapabilityId::from_bytes(parse_hex16(value)?)
        .map_err(|_| InstallationPlanError::new(InstallationPlanErrorKind::InvalidEncoding))
}
