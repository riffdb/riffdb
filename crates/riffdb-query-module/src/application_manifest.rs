//! Canonical, versioned application-manifest model.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use riffdb_types::{
    ApplicationManifestHash, ContractBundleHash, QueryModuleHash, ReactiveModuleHash,
    hash_application_manifest,
};
use serde_json::{Map, Value, json};

/// Exact manifest schema identifier.
pub const APPLICATION_MANIFEST_SCHEMA_V1: &str = "riffdb.application-manifest/v1";
/// Exact manifest schema binding immutable reactive modules.
pub const APPLICATION_MANIFEST_SCHEMA_V2: &str = "riffdb.application-manifest/v2";
/// Exact manifest schema binding an exact generated Go target.
pub const APPLICATION_MANIFEST_SCHEMA_V3: &str = "riffdb.application-manifest/v3";
/// Exact manifest schema binding compiler-owned row policies to roles.
pub const APPLICATION_MANIFEST_SCHEMA_V4: &str = "riffdb.application-manifest/v4";
/// Exact manifest schema with a closed sparse generated-surface declaration.
pub const APPLICATION_MANIFEST_SCHEMA_V5: &str = "riffdb.application-manifest/v5";
/// Maximum accepted application-manifest source bytes.
pub const MAX_APPLICATION_MANIFEST_BYTES: usize = 1_048_576;
const MAX_NAME_BYTES: usize = 256;
const MAX_PATH_BYTES: usize = 512;
const MAX_QUERY_MODULES: usize = 32;
const MAX_REACTIVE_MODULES: usize = 32;
const MAX_QUERY_SOURCES: usize = 4_096;
const MAX_ROLES: usize = 128;
const MAX_ROLE_OPERATIONS: usize = 4_096;
const MAX_SEED_INPUTS: usize = 256;

/// One half-open byte span in canonical manifest source.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ManifestSpan {
    start: usize,
    end: usize,
}

impl ManifestSpan {
    /// Inclusive start byte.
    #[must_use]
    pub const fn start(self) -> usize {
        self.start
    }

    /// Exclusive end byte.
    #[must_use]
    pub const fn end(self) -> usize {
        self.end
    }
}

/// Bounded source locations for manifest-defined symbols and paths.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ApplicationManifestSourceMap {
    spans: BTreeMap<String, ManifestSpan>,
}

impl ApplicationManifestSourceMap {
    /// Looks up one canonical semantic location such as `contract.source`.
    #[must_use]
    pub fn span(&self, path: &str) -> Option<ManifestSpan> {
        self.spans.get(path).copied()
    }
}

/// Exact contract source and compiled identity selected by an application.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ManifestContract {
    source: String,
    lineage: String,
    version: u64,
    bundle_hash: ContractBundleHash,
}

impl ManifestContract {
    /// Workspace-relative contract source.
    #[must_use]
    pub fn source(&self) -> &str {
        &self.source
    }

    /// Exact contract lineage.
    #[must_use]
    pub fn lineage(&self) -> &str {
        &self.lineage
    }

    /// Exact positive contract version.
    #[must_use]
    pub const fn version(&self) -> u64 {
        self.version
    }

    /// Exact compiled bundle hash.
    #[must_use]
    pub const fn bundle_hash(&self) -> ContractBundleHash {
        self.bundle_hash
    }
}

/// One named RiffQL source in a manifest module.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ManifestQuerySource {
    name: String,
    source: String,
}

impl ManifestQuerySource {
    /// Exact named query.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Workspace-relative `.riffq` source.
    #[must_use]
    pub fn source(&self) -> &str {
        &self.source
    }
}

/// One immutable named-query module binding.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ManifestQueryModule {
    name: String,
    version: u64,
    module_hash: QueryModuleHash,
    queries: Vec<ManifestQuerySource>,
}

impl ManifestQueryModule {
    /// Module name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Positive module version.
    #[must_use]
    pub const fn version(&self) -> u64 {
        self.version
    }

    /// Exact immutable module hash.
    #[must_use]
    pub const fn module_hash(&self) -> QueryModuleHash {
        self.module_hash
    }

    /// Named query sources in canonical name order.
    #[must_use]
    pub fn queries(&self) -> &[ManifestQuerySource] {
        &self.queries
    }
}

/// One immutable reactive-module binding.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ManifestReactiveModule {
    name: String,
    version: u64,
    module_hash: ReactiveModuleHash,
    source: String,
}

impl ManifestReactiveModule {
    /// Module name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }
    /// Positive version.
    #[must_use]
    pub const fn version(&self) -> u64 {
        self.version
    }
    /// Exact module identity.
    #[must_use]
    pub const fn module_hash(&self) -> ReactiveModuleHash {
        self.module_hash
    }
    /// Workspace-relative source path.
    #[must_use]
    pub fn source(&self) -> &str {
        &self.source
    }
}

/// One symbolic application role.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ManifestRole {
    name: String,
    environment: String,
    tenant_scope: ManifestTenantScope,
    queries: Vec<String>,
    commands: Vec<String>,
    event_streams: Vec<String>,
    watch_queries: Vec<String>,
    agent_subscriptions: Vec<String>,
    row_policies: Vec<String>,
}

/// Symbolic tenant binding required by an application role.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ManifestTenantScope {
    /// The role is bound only for global application operations.
    Global,
    /// A concrete tenant must be supplied when the role is bound.
    Tenant,
}

impl ManifestTenantScope {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Global => "global",
            Self::Tenant => "tenant",
        }
    }
}

impl ManifestRole {
    /// Role name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Exact deployment environment selected by this role.
    #[must_use]
    pub fn environment(&self) -> &str {
        &self.environment
    }

    /// Whether binding is global or requires one exact tenant.
    #[must_use]
    pub const fn tenant_scope(&self) -> ManifestTenantScope {
        self.tenant_scope
    }

    /// Exact named-query allowlist.
    #[must_use]
    pub fn queries(&self) -> &[String] {
        &self.queries
    }

    /// Exact symbolic-command allowlist.
    #[must_use]
    pub fn commands(&self) -> &[String] {
        &self.commands
    }
    /// Exact event-stream allowlist.
    #[must_use]
    pub fn event_streams(&self) -> &[String] {
        &self.event_streams
    }
    /// Exact query-watch allowlist.
    #[must_use]
    pub fn watch_queries(&self) -> &[String] {
        &self.watch_queries
    }
    /// Exact contextual-subscription allowlist.
    #[must_use]
    pub fn agent_subscriptions(&self) -> &[String] {
        &self.agent_subscriptions
    }
    /// Exact symbolic row-policy allowlist.
    #[must_use]
    pub fn row_policies(&self) -> &[String] {
        &self.row_policies
    }
}

/// Generated artifact output roots.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ManifestGenerationTargets {
    rust: Option<String>,
    typescript: Option<String>,
    go: Option<String>,
    mcp: Option<String>,
    python: Option<String>,
}

impl ManifestGenerationTargets {
    /// Exact path for one compiler-owned generated surface.
    #[must_use]
    pub fn path(&self, surface: crate::GeneratedApplicationSurface) -> Option<&str> {
        match surface {
            crate::GeneratedApplicationSurface::Rust => self.rust(),
            crate::GeneratedApplicationSurface::Go => self.go(),
            crate::GeneratedApplicationSurface::TypeScript => self.typescript(),
            crate::GeneratedApplicationSurface::Python => self.python(),
            crate::GeneratedApplicationSurface::Mcp => self.mcp(),
        }
    }

    /// Declared surfaces and paths in compiler-registry order.
    pub fn declared(
        &self,
    ) -> impl Iterator<Item = (crate::GeneratedApplicationSurface, &str)> + '_ {
        crate::GeneratedApplicationSurface::ALL
            .into_iter()
            .filter_map(|surface| self.path(surface).map(|path| (surface, path)))
    }

    /// Workspace-relative Rust output.
    #[must_use]
    pub fn rust(&self) -> Option<&str> {
        self.rust.as_deref()
    }

    /// Workspace-relative TypeScript output.
    #[must_use]
    pub fn typescript(&self) -> Option<&str> {
        self.typescript.as_deref()
    }

    /// Workspace-relative Go output when present.
    #[must_use]
    pub fn go(&self) -> Option<&str> {
        self.go.as_deref()
    }

    /// Workspace-relative MCP-schema output.
    #[must_use]
    pub fn mcp(&self) -> Option<&str> {
        self.mcp.as_deref()
    }
    /// Workspace-relative Python output when present.
    #[must_use]
    pub fn python(&self) -> Option<&str> {
        self.python.as_deref()
    }
}

/// One exact canonical application manifest.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ApplicationManifest {
    schema: &'static str,
    application_name: String,
    contract: ManifestContract,
    query_modules: Vec<ManifestQueryModule>,
    reactive_modules: Vec<ManifestReactiveModule>,
    roles: Vec<ManifestRole>,
    generation: ManifestGenerationTargets,
    seed_inputs: Vec<String>,
    canonical_bytes: Vec<u8>,
    identity: ApplicationManifestHash,
    source_map: ApplicationManifestSourceMap,
}

impl ApplicationManifest {
    /// Parses bounded JSON, validates its closed v1 grammar, and canonicalizes it.
    pub fn parse(source: &str) -> Result<Self, ManifestError> {
        if source.is_empty() || source.len() > MAX_APPLICATION_MANIFEST_BYTES {
            return Err(ManifestError::new(ManifestErrorKind::LimitExceeded));
        }
        let value: Value = serde_json::from_str(source)
            .map_err(|_| ManifestError::new(ManifestErrorKind::InvalidJson))?;
        let schema_value = value
            .as_object()
            .and_then(|root| root.get("schema"))
            .and_then(Value::as_str)
            .ok_or_else(|| ManifestError::new(ManifestErrorKind::InvalidShape))?;
        let root_keys = if matches!(
            schema_value,
            APPLICATION_MANIFEST_SCHEMA_V2
                | APPLICATION_MANIFEST_SCHEMA_V3
                | APPLICATION_MANIFEST_SCHEMA_V4
                | APPLICATION_MANIFEST_SCHEMA_V5
        ) {
            &[
                "application",
                "contract",
                "generation",
                "query_modules",
                "reactive_modules",
                "roles",
                "schema",
                "seed_inputs",
            ][..]
        } else {
            &[
                "application",
                "contract",
                "generation",
                "query_modules",
                "roles",
                "schema",
                "seed_inputs",
            ][..]
        };
        let root = object(&value, root_keys)?;
        let schema = match string(root, "schema")? {
            APPLICATION_MANIFEST_SCHEMA_V1 => APPLICATION_MANIFEST_SCHEMA_V1,
            APPLICATION_MANIFEST_SCHEMA_V2 => APPLICATION_MANIFEST_SCHEMA_V2,
            APPLICATION_MANIFEST_SCHEMA_V3 => APPLICATION_MANIFEST_SCHEMA_V3,
            APPLICATION_MANIFEST_SCHEMA_V4 => APPLICATION_MANIFEST_SCHEMA_V4,
            APPLICATION_MANIFEST_SCHEMA_V5 => APPLICATION_MANIFEST_SCHEMA_V5,
            _ => return Err(ManifestError::new(ManifestErrorKind::UnsupportedVersion)),
        };
        let application_name = checked_name(string(root, "application")?)?;
        let contract = parse_contract(required(root, "contract")?)?;
        let query_modules = parse_query_modules(required(root, "query_modules")?)?;
        let reactive_modules = if matches!(
            schema,
            APPLICATION_MANIFEST_SCHEMA_V2
                | APPLICATION_MANIFEST_SCHEMA_V3
                | APPLICATION_MANIFEST_SCHEMA_V4
                | APPLICATION_MANIFEST_SCHEMA_V5
        ) {
            parse_reactive_modules(required(root, "reactive_modules")?)?
        } else {
            Vec::new()
        };
        let roles = parse_roles(required(root, "roles")?, &query_modules, schema)?;
        let generation = parse_generation(required(root, "generation")?, schema)?;
        let seed_inputs = parse_paths(required(root, "seed_inputs")?, MAX_SEED_INPUTS)?;

        let mut canonical_value = json!({
            "application": application_name,
            "contract": {
                "bundle_hash": hex(contract.bundle_hash.as_bytes()),
                "lineage": contract.lineage,
                "source": contract.source,
                "version": contract.version,
            },
            "generation": {
                "mcp": generation.mcp,
                "rust": generation.rust,
                "typescript": generation.typescript,
            },
            "query_modules": query_modules.iter().map(|module| json!({
                "module_hash": hex(module.module_hash.as_bytes()),
                "name": module.name,
                "queries": module.queries.iter().map(|query| json!({
                    "name": query.name,
                    "source": query.source,
                })).collect::<Vec<_>>(),
                "version": module.version,
            })).collect::<Vec<_>>(),
            "roles": roles.iter().map(|role| if matches!(schema, APPLICATION_MANIFEST_SCHEMA_V2 | APPLICATION_MANIFEST_SCHEMA_V3 | APPLICATION_MANIFEST_SCHEMA_V4 | APPLICATION_MANIFEST_SCHEMA_V5) {
                manifest_role_value(role, schema)
            } else {
                json!({"commands": role.commands, "environment": role.environment,
                    "name": role.name, "queries": role.queries,
                    "tenant_scope": role.tenant_scope.as_str()})
            }).collect::<Vec<_>>(),
            "schema": schema,
            "seed_inputs": seed_inputs,
        });
        if matches!(
            schema,
            APPLICATION_MANIFEST_SCHEMA_V2
                | APPLICATION_MANIFEST_SCHEMA_V3
                | APPLICATION_MANIFEST_SCHEMA_V4
                | APPLICATION_MANIFEST_SCHEMA_V5
        ) {
            let root = canonical_value.as_object_mut().expect("manifest object");
            let generation_value = generation
                .declared()
                .map(|(surface, path)| (surface.key().to_owned(), json!(path)))
                .collect::<Map<_, _>>();
            root.insert("generation".to_owned(), Value::Object(generation_value));
            root.insert(
                "reactive_modules".to_owned(),
                json!(
                    reactive_modules
                        .iter()
                        .map(|module| json!({
                            "module_hash": hex(module.module_hash.as_bytes()), "name": module.name,
                            "source": module.source, "version": module.version,
                        }))
                        .collect::<Vec<_>>()
                ),
            );
        }
        let mut canonical_bytes = serde_json::to_vec(&canonical_value)
            .map_err(|_| ManifestError::new(ManifestErrorKind::InvalidJson))?;
        canonical_bytes.push(b'\n');
        if canonical_bytes.len() > MAX_APPLICATION_MANIFEST_BYTES {
            return Err(ManifestError::new(ManifestErrorKind::LimitExceeded));
        }
        let source_map = build_source_map(
            &canonical_bytes,
            &contract,
            &query_modules,
            &roles,
            &generation,
            &seed_inputs,
        )?;
        let identity = hash_application_manifest(&canonical_bytes);
        Ok(Self {
            schema,
            application_name,
            contract,
            query_modules,
            reactive_modules,
            roles,
            generation,
            seed_inputs,
            canonical_bytes,
            identity,
            source_map,
        })
    }

    /// Strictly decodes canonical v1 bytes and rejects alternate spellings.
    pub fn decode_canonical(bytes: &[u8]) -> Result<Self, ManifestError> {
        let source = std::str::from_utf8(bytes)
            .map_err(|_| ManifestError::new(ManifestErrorKind::InvalidJson))?;
        let manifest = Self::parse(source)?;
        if manifest.canonical_bytes != bytes {
            return Err(ManifestError::new(ManifestErrorKind::NonCanonical));
        }
        Ok(manifest)
    }

    /// Application package name.
    #[must_use]
    pub fn application_name(&self) -> &str {
        &self.application_name
    }

    /// Exact manifest schema.
    #[must_use]
    pub const fn schema(&self) -> &'static str {
        self.schema
    }

    /// Exact contract binding.
    #[must_use]
    pub const fn contract(&self) -> &ManifestContract {
        &self.contract
    }

    /// Query modules in canonical name order.
    #[must_use]
    pub fn query_modules(&self) -> &[ManifestQueryModule] {
        &self.query_modules
    }

    /// Reactive modules in canonical name order.
    #[must_use]
    pub fn reactive_modules(&self) -> &[ManifestReactiveModule] {
        &self.reactive_modules
    }

    /// Symbolic roles in canonical name order.
    #[must_use]
    pub fn roles(&self) -> &[ManifestRole] {
        &self.roles
    }

    /// Generated artifact output roots.
    #[must_use]
    pub const fn generation(&self) -> &ManifestGenerationTargets {
        &self.generation
    }

    /// Seed input paths in canonical order.
    #[must_use]
    pub fn seed_inputs(&self) -> &[String] {
        &self.seed_inputs
    }

    /// Canonical JSON bytes, including one trailing line feed.
    #[must_use]
    pub fn canonical_bytes(&self) -> &[u8] {
        &self.canonical_bytes
    }

    /// Domain-separated manifest identity.
    #[must_use]
    pub const fn identity(&self) -> ApplicationManifestHash {
        self.identity
    }

    /// Source locations in canonical bytes.
    #[must_use]
    pub const fn source_map(&self) -> &ApplicationManifestSourceMap {
        &self.source_map
    }
}

/// Closed application-manifest failure classification.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ManifestErrorKind {
    /// Source is not valid JSON.
    InvalidJson,
    /// The manifest version is not supported.
    UnsupportedVersion,
    /// A required field is absent or has the wrong JSON type.
    InvalidShape,
    /// A name is empty or outside the accepted symbolic grammar.
    InvalidName,
    /// A path is absolute, escapes the workspace, or is otherwise invalid.
    InvalidPath,
    /// A declared symbol or path is duplicated.
    Duplicate,
    /// A role names an unknown query.
    UnknownOperation,
    /// A fixed manifest bound was exceeded.
    LimitExceeded,
    /// Strict decoding observed a noncanonical spelling or order.
    NonCanonical,
}

/// Safe bounded manifest error.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ManifestError {
    kind: ManifestErrorKind,
}

impl ManifestError {
    const fn new(kind: ManifestErrorKind) -> Self {
        Self { kind }
    }

    /// Closed failure kind.
    #[must_use]
    pub const fn kind(self) -> ManifestErrorKind {
        self.kind
    }
}

impl fmt::Display for ManifestError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self.kind {
            ManifestErrorKind::InvalidJson => "application manifest JSON is invalid",
            ManifestErrorKind::UnsupportedVersion => "application manifest version is unsupported",
            ManifestErrorKind::InvalidShape => "application manifest shape is invalid",
            ManifestErrorKind::InvalidName => "application manifest name is invalid",
            ManifestErrorKind::InvalidPath => "application manifest path is invalid",
            ManifestErrorKind::Duplicate => "application manifest contains a duplicate",
            ManifestErrorKind::UnknownOperation => {
                "application role names an unknown application operation"
            }
            ManifestErrorKind::LimitExceeded => "application manifest limit exceeded",
            ManifestErrorKind::NonCanonical => "application manifest bytes are not canonical",
        })
    }
}

impl std::error::Error for ManifestError {}

fn parse_contract(value: &Value) -> Result<ManifestContract, ManifestError> {
    let object = object(value, &["bundle_hash", "lineage", "source", "version"])?;
    let version = positive_u64(object, "version")?;
    Ok(ManifestContract {
        source: checked_path(string(object, "source")?)?,
        lineage: checked_name(string(object, "lineage")?)?,
        version,
        bundle_hash: ContractBundleHash::from_bytes(parse_hash(string(object, "bundle_hash")?)?),
    })
}

fn parse_query_modules(value: &Value) -> Result<Vec<ManifestQueryModule>, ManifestError> {
    let values = array(value, 1, MAX_QUERY_MODULES)?;
    let mut modules = Vec::with_capacity(values.len());
    for value in values {
        let module_object = object(value, &["module_hash", "name", "queries", "version"])?;
        let query_values = array(required(module_object, "queries")?, 0, MAX_QUERY_SOURCES)?;
        let mut queries = Vec::with_capacity(query_values.len());
        for query in query_values {
            let query = object(query, &["name", "source"])?;
            queries.push(ManifestQuerySource {
                name: checked_name(string(query, "name")?)?,
                source: checked_path(string(query, "source")?)?,
            });
        }
        queries.sort_by(|left, right| left.name.cmp(&right.name));
        ensure_unique(queries.iter().map(|query| query.name.as_str()))?;
        ensure_unique(queries.iter().map(|query| query.source.as_str()))?;
        modules.push(ManifestQueryModule {
            name: checked_name(string(module_object, "name")?)?,
            version: positive_u64(module_object, "version")?,
            module_hash: QueryModuleHash::from_bytes(parse_hash(string(
                module_object,
                "module_hash",
            )?)?),
            queries,
        });
    }
    modules.sort_by(|left, right| left.name.cmp(&right.name));
    ensure_unique(modules.iter().map(|module| module.name.as_str()))?;
    Ok(modules)
}

fn parse_reactive_modules(value: &Value) -> Result<Vec<ManifestReactiveModule>, ManifestError> {
    let values = array(value, 0, MAX_REACTIVE_MODULES)?;
    let mut modules = values
        .iter()
        .map(|value| {
            let object = object(value, &["module_hash", "name", "source", "version"])?;
            Ok(ManifestReactiveModule {
                name: checked_name(string(object, "name")?)?,
                version: positive_u64(object, "version")?,
                module_hash: ReactiveModuleHash::from_bytes(parse_hash(string(
                    object,
                    "module_hash",
                )?)?),
                source: checked_path(string(object, "source")?)?,
            })
        })
        .collect::<Result<Vec<_>, ManifestError>>()?;
    modules.sort_by(|left, right| left.name.cmp(&right.name));
    ensure_unique(modules.iter().map(|module| module.name.as_str()))?;
    ensure_unique(modules.iter().map(|module| module.source.as_str()))?;
    Ok(modules)
}

fn parse_roles(
    value: &Value,
    modules: &[ManifestQueryModule],
    schema: &str,
) -> Result<Vec<ManifestRole>, ManifestError> {
    let values = array(value, 0, MAX_ROLES)?;
    let available_queries = modules
        .iter()
        .flat_map(|module| module.queries.iter().map(|query| query.name.as_str()))
        .collect::<BTreeSet<_>>();
    let mut roles = Vec::with_capacity(values.len());
    for value in values {
        let object = if matches!(
            schema,
            APPLICATION_MANIFEST_SCHEMA_V4 | APPLICATION_MANIFEST_SCHEMA_V5
        ) {
            object(
                value,
                &[
                    "agent_subscriptions",
                    "commands",
                    "environment",
                    "event_streams",
                    "name",
                    "queries",
                    "row_policies",
                    "tenant_scope",
                    "watch_queries",
                ],
            )?
        } else if matches!(
            schema,
            APPLICATION_MANIFEST_SCHEMA_V2 | APPLICATION_MANIFEST_SCHEMA_V3
        ) {
            object(
                value,
                &[
                    "agent_subscriptions",
                    "commands",
                    "environment",
                    "event_streams",
                    "name",
                    "queries",
                    "tenant_scope",
                    "watch_queries",
                ],
            )?
        } else {
            object(
                value,
                &["commands", "environment", "name", "queries", "tenant_scope"],
            )?
        };
        let mut queries = parse_names(required(object, "queries")?, MAX_ROLE_OPERATIONS)?;
        let mut commands = parse_names(required(object, "commands")?, MAX_ROLE_OPERATIONS)?;
        let mut event_streams = if matches!(
            schema,
            APPLICATION_MANIFEST_SCHEMA_V2
                | APPLICATION_MANIFEST_SCHEMA_V3
                | APPLICATION_MANIFEST_SCHEMA_V4
                | APPLICATION_MANIFEST_SCHEMA_V5
        ) {
            parse_names(required(object, "event_streams")?, MAX_ROLE_OPERATIONS)?
        } else {
            Vec::new()
        };
        let mut watch_queries = if matches!(
            schema,
            APPLICATION_MANIFEST_SCHEMA_V2
                | APPLICATION_MANIFEST_SCHEMA_V3
                | APPLICATION_MANIFEST_SCHEMA_V4
                | APPLICATION_MANIFEST_SCHEMA_V5
        ) {
            parse_names(required(object, "watch_queries")?, MAX_ROLE_OPERATIONS)?
        } else {
            Vec::new()
        };
        let mut agent_subscriptions = if matches!(
            schema,
            APPLICATION_MANIFEST_SCHEMA_V2
                | APPLICATION_MANIFEST_SCHEMA_V3
                | APPLICATION_MANIFEST_SCHEMA_V4
                | APPLICATION_MANIFEST_SCHEMA_V5
        ) {
            parse_names(
                required(object, "agent_subscriptions")?,
                MAX_ROLE_OPERATIONS,
            )?
        } else {
            Vec::new()
        };
        let mut row_policies = if matches!(
            schema,
            APPLICATION_MANIFEST_SCHEMA_V4 | APPLICATION_MANIFEST_SCHEMA_V5
        ) {
            parse_names(required(object, "row_policies")?, MAX_ROLE_OPERATIONS)?
        } else {
            Vec::new()
        };
        queries.sort();
        commands.sort();
        event_streams.sort();
        watch_queries.sort();
        agent_subscriptions.sort();
        row_policies.sort();
        ensure_unique(queries.iter().map(String::as_str))?;
        ensure_unique(commands.iter().map(String::as_str))?;
        ensure_unique(event_streams.iter().map(String::as_str))?;
        ensure_unique(watch_queries.iter().map(String::as_str))?;
        ensure_unique(agent_subscriptions.iter().map(String::as_str))?;
        ensure_unique(row_policies.iter().map(String::as_str))?;
        if queries
            .iter()
            .any(|query| !available_queries.contains(query.as_str()))
        {
            return Err(ManifestError::new(ManifestErrorKind::UnknownOperation));
        }
        roles.push(ManifestRole {
            name: checked_name(string(object, "name")?)?,
            environment: checked_name(string(object, "environment")?)?,
            tenant_scope: match string(object, "tenant_scope")? {
                "global" => ManifestTenantScope::Global,
                "tenant" => ManifestTenantScope::Tenant,
                _ => return Err(ManifestError::new(ManifestErrorKind::InvalidShape)),
            },
            queries,
            commands,
            event_streams,
            watch_queries,
            agent_subscriptions,
            row_policies,
        });
    }
    roles.sort_by(|left, right| left.name.cmp(&right.name));
    ensure_unique(roles.iter().map(|role| role.name.as_str()))?;
    Ok(roles)
}

fn manifest_role_value(role: &ManifestRole, schema: &str) -> Value {
    let mut value = json!({
        "agent_subscriptions": role.agent_subscriptions,
        "commands": role.commands,
        "environment": role.environment,
        "event_streams": role.event_streams,
        "name": role.name,
        "queries": role.queries,
        "tenant_scope": role.tenant_scope.as_str(),
        "watch_queries": role.watch_queries,
    });
    if matches!(
        schema,
        APPLICATION_MANIFEST_SCHEMA_V4 | APPLICATION_MANIFEST_SCHEMA_V5
    ) {
        value
            .as_object_mut()
            .expect("compiler-created role object")
            .insert("row_policies".to_owned(), json!(role.row_policies));
    }
    value
}

fn parse_generation(
    value: &Value,
    schema: &str,
) -> Result<ManifestGenerationTargets, ManifestError> {
    let object = if schema == APPLICATION_MANIFEST_SCHEMA_V5 {
        let object = value
            .as_object()
            .ok_or_else(|| ManifestError::new(ManifestErrorKind::InvalidShape))?;
        if object.is_empty()
            || object.len() > 5
            || object
                .keys()
                .any(|key| crate::GeneratedApplicationSurface::parse(key).is_none())
        {
            return Err(ManifestError::new(ManifestErrorKind::InvalidShape));
        }
        object
    } else if matches!(
        schema,
        APPLICATION_MANIFEST_SCHEMA_V3 | APPLICATION_MANIFEST_SCHEMA_V4
    ) {
        object(value, &["go", "mcp", "python", "rust", "typescript"])?
    } else if schema == APPLICATION_MANIFEST_SCHEMA_V2 {
        object(value, &["mcp", "python", "rust", "typescript"])?
    } else {
        object(value, &["mcp", "rust", "typescript"])?
    };
    let optional_path = |key: &str| {
        object
            .get(key)
            .map(|value| {
                value
                    .as_str()
                    .ok_or_else(|| ManifestError::new(ManifestErrorKind::InvalidShape))
            })
            .transpose()?
            .map(checked_path)
            .transpose()
    };
    let generation = ManifestGenerationTargets {
        rust: if schema == APPLICATION_MANIFEST_SCHEMA_V5 {
            optional_path("rust")?
        } else {
            Some(checked_path(string(object, "rust")?)?)
        },
        typescript: if schema == APPLICATION_MANIFEST_SCHEMA_V5 {
            optional_path("typescript")?
        } else {
            Some(checked_path(string(object, "typescript")?)?)
        },
        go: if schema == APPLICATION_MANIFEST_SCHEMA_V5 {
            optional_path("go")?
        } else if matches!(
            schema,
            APPLICATION_MANIFEST_SCHEMA_V3 | APPLICATION_MANIFEST_SCHEMA_V4
        ) {
            Some(checked_path(string(object, "go")?)?)
        } else {
            None
        },
        mcp: if schema == APPLICATION_MANIFEST_SCHEMA_V5 {
            optional_path("mcp")?
        } else {
            Some(checked_path(string(object, "mcp")?)?)
        },
        python: if schema == APPLICATION_MANIFEST_SCHEMA_V5 {
            optional_path("python")?
        } else if matches!(
            schema,
            APPLICATION_MANIFEST_SCHEMA_V2
                | APPLICATION_MANIFEST_SCHEMA_V3
                | APPLICATION_MANIFEST_SCHEMA_V4
        ) {
            Some(checked_path(string(object, "python")?)?)
        } else {
            None
        },
    };
    ensure_unique(
        [
            generation.go.as_deref(),
            generation.mcp.as_deref(),
            generation.python.as_deref(),
            generation.rust.as_deref(),
            generation.typescript.as_deref(),
        ]
        .into_iter()
        .flatten(),
    )?;
    Ok(generation)
}

fn parse_paths(value: &Value, maximum: usize) -> Result<Vec<String>, ManifestError> {
    let mut paths = array(value, 0, maximum)?
        .iter()
        .map(|value| {
            value
                .as_str()
                .ok_or_else(|| ManifestError::new(ManifestErrorKind::InvalidShape))
                .and_then(checked_path)
        })
        .collect::<Result<Vec<_>, _>>()?;
    paths.sort();
    ensure_unique(paths.iter().map(String::as_str))?;
    Ok(paths)
}

fn parse_names(value: &Value, maximum: usize) -> Result<Vec<String>, ManifestError> {
    array(value, 0, maximum)?
        .iter()
        .map(|value| {
            value
                .as_str()
                .ok_or_else(|| ManifestError::new(ManifestErrorKind::InvalidShape))
                .and_then(checked_name)
        })
        .collect()
}

fn checked_name(value: &str) -> Result<String, ManifestError> {
    if value.is_empty()
        || value.len() > MAX_NAME_BYTES
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
        || !value
            .as_bytes()
            .first()
            .is_some_and(u8::is_ascii_alphabetic)
    {
        return Err(ManifestError::new(ManifestErrorKind::InvalidName));
    }
    Ok(value.to_owned())
}

fn checked_path(value: &str) -> Result<String, ManifestError> {
    if value.is_empty()
        || value.len() > MAX_PATH_BYTES
        || value.starts_with('/')
        || value.starts_with('\\')
        || value.contains('\\')
        || value.split('/').any(|part| {
            part.is_empty()
                || part == "."
                || part == ".."
                || part.bytes().any(|byte| byte.is_ascii_control())
        })
    {
        return Err(ManifestError::new(ManifestErrorKind::InvalidPath));
    }
    Ok(value.to_owned())
}

fn parse_hash(value: &str) -> Result<[u8; 32], ManifestError> {
    if value.len() != 64 {
        return Err(ManifestError::new(ManifestErrorKind::InvalidShape));
    }
    let mut bytes = [0_u8; 32];
    for (index, pair) in value.as_bytes().chunks_exact(2).enumerate() {
        let pair = std::str::from_utf8(pair)
            .map_err(|_| ManifestError::new(ManifestErrorKind::InvalidShape))?;
        bytes[index] = u8::from_str_radix(pair, 16)
            .map_err(|_| ManifestError::new(ManifestErrorKind::InvalidShape))?;
    }
    Ok(bytes)
}

fn required<'a>(object: &'a Map<String, Value>, key: &str) -> Result<&'a Value, ManifestError> {
    object
        .get(key)
        .ok_or_else(|| ManifestError::new(ManifestErrorKind::InvalidShape))
}

fn string<'a>(object: &'a Map<String, Value>, key: &str) -> Result<&'a str, ManifestError> {
    required(object, key)?
        .as_str()
        .ok_or_else(|| ManifestError::new(ManifestErrorKind::InvalidShape))
}

fn positive_u64(object: &Map<String, Value>, key: &str) -> Result<u64, ManifestError> {
    let value = required(object, key)?
        .as_u64()
        .ok_or_else(|| ManifestError::new(ManifestErrorKind::InvalidShape))?;
    if value == 0 {
        return Err(ManifestError::new(ManifestErrorKind::InvalidShape));
    }
    Ok(value)
}

fn object<'a>(
    value: &'a Value,
    expected_keys: &[&str],
) -> Result<&'a Map<String, Value>, ManifestError> {
    let object = value
        .as_object()
        .ok_or_else(|| ManifestError::new(ManifestErrorKind::InvalidShape))?;
    if object.len() != expected_keys.len()
        || !expected_keys.iter().all(|key| object.contains_key(*key))
    {
        return Err(ManifestError::new(ManifestErrorKind::InvalidShape));
    }
    Ok(object)
}

fn array(value: &Value, minimum: usize, maximum: usize) -> Result<&[Value], ManifestError> {
    let values = value
        .as_array()
        .ok_or_else(|| ManifestError::new(ManifestErrorKind::InvalidShape))?;
    if values.len() < minimum || values.len() > maximum {
        return Err(ManifestError::new(ManifestErrorKind::LimitExceeded));
    }
    Ok(values)
}

fn ensure_unique<'a>(values: impl IntoIterator<Item = &'a str>) -> Result<(), ManifestError> {
    let mut seen = BTreeSet::new();
    if values.into_iter().any(|value| !seen.insert(value)) {
        return Err(ManifestError::new(ManifestErrorKind::Duplicate));
    }
    Ok(())
}

fn build_source_map(
    canonical: &[u8],
    contract: &ManifestContract,
    modules: &[ManifestQueryModule],
    roles: &[ManifestRole],
    generation: &ManifestGenerationTargets,
    seeds: &[String],
) -> Result<ApplicationManifestSourceMap, ManifestError> {
    let mut requested = vec![
        ("contract.source".to_owned(), contract.source.as_str()),
        ("contract.lineage".to_owned(), contract.lineage.as_str()),
    ];
    if let Some(rust) = generation.rust.as_deref() {
        requested.push(("generation.rust".to_owned(), rust));
    }
    if let Some(typescript) = generation.typescript.as_deref() {
        requested.push(("generation.typescript".to_owned(), typescript));
    }
    if let Some(mcp) = generation.mcp.as_deref() {
        requested.push(("generation.mcp".to_owned(), mcp));
    }
    if let Some(go) = generation.go.as_deref() {
        requested.push(("generation.go".to_owned(), go));
    }
    if let Some(python) = generation.python.as_deref() {
        requested.push(("generation.python".to_owned(), python));
    }
    for module in modules {
        requested.push((
            format!("query_modules.{}.name", module.name),
            module.name.as_str(),
        ));
        for query in &module.queries {
            requested.push((
                format!(
                    "query_modules.{}.queries.{}.source",
                    module.name, query.name
                ),
                query.source.as_str(),
            ));
        }
    }
    for role in roles {
        requested.push((format!("roles.{}.name", role.name), role.name.as_str()));
    }
    for (index, seed) in seeds.iter().enumerate() {
        requested.push((format!("seed_inputs.{index}"), seed.as_str()));
    }
    let source = std::str::from_utf8(canonical)
        .map_err(|_| ManifestError::new(ManifestErrorKind::InvalidJson))?;
    let mut spans = BTreeMap::new();
    for (path, value) in requested {
        let encoded = serde_json::to_string(value)
            .map_err(|_| ManifestError::new(ManifestErrorKind::InvalidJson))?;
        let start = source
            .find(&encoded)
            .ok_or_else(|| ManifestError::new(ManifestErrorKind::InvalidShape))?;
        spans.insert(
            path,
            ManifestSpan {
                start,
                end: start + encoded.len(),
            },
        );
    }
    for role in roles {
        let encoded_environment = serde_json::to_string(&role.environment)
            .map_err(|_| ManifestError::new(ManifestErrorKind::InvalidJson))?;
        let encoded_name = serde_json::to_string(&role.name)
            .map_err(|_| ManifestError::new(ManifestErrorKind::InvalidJson))?;
        let name_marker = format!("\"name\":{encoded_name}");
        let name_start = source
            .find(&name_marker)
            .ok_or_else(|| ManifestError::new(ManifestErrorKind::InvalidShape))?;
        let environment_marker = format!("\"environment\":{encoded_environment}");
        let marker_start = source[..name_start]
            .rfind(&environment_marker)
            .ok_or_else(|| ManifestError::new(ManifestErrorKind::InvalidShape))?;
        let start = marker_start + "\"environment\":".len();
        spans.insert(
            format!("roles.{}.environment", role.name),
            ManifestSpan {
                start,
                end: start + encoded_environment.len(),
            },
        );
    }
    Ok(ApplicationManifestSourceMap { spans })
}

fn hex(bytes: &[u8; 32]) -> String {
    use std::fmt::Write as _;

    let mut output = String::with_capacity(64);
    for byte in bytes {
        write!(output, "{byte:02x}").expect("writing to String cannot fail");
    }
    output
}
