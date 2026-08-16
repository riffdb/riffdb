use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use riffdb_types::hash_generated_artifact;
use serde::Deserialize;
use serde_json::Value;
use sha2::{Digest, Sha256};

const MAX_ARTIFACT_BYTES: usize = 8 * 1024 * 1024;
const MAX_OPERATIONS: usize = 4_096;

/// Exact generated application operation class.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OperationKind {
    /// Compiled command.
    Command,
    /// Named RiffQL query.
    Query,
    /// Generated reactive action.
    Reactive,
}

/// Closed generated reactive operation class.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum ReactiveKind {
    /// Durable domain-event stream.
    Stream,
    /// Live named-query watch.
    Watch,
    /// Contextual agent work subscription.
    Subscription,
}

/// One exact generated operation safe for local dispatch.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OperationSpec {
    public_name: String,
    symbol: String,
    kind: OperationKind,
    module_hash: Option<[u8; 32]>,
    plan_hash: Option<[u8; 32]>,
    input_schema_hash: [u8; 32],
    result_schema_hash: [u8; 32],
    reactive_kind: Option<ReactiveKind>,
    reactive_action: Option<String>,
    reaction: Option<ReactionSpec>,
}

/// Exact generated contextual reaction target.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReactionSpec {
    name: String,
    command_name: String,
    command_id: u32,
}

impl ReactionSpec {
    /// Declared subscription-local reaction symbol.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }
    /// Exact target command symbol.
    #[must_use]
    pub fn command_name(&self) -> &str {
        &self.command_name
    }
    /// Stable compiler-owned target command identity.
    #[must_use]
    pub const fn command_id(&self) -> u32 {
        self.command_id
    }
}

impl OperationSpec {
    /// Generated local operation name.
    #[must_use]
    pub fn public_name(&self) -> &str {
        &self.public_name
    }
    /// Contract/query symbol, or exact generated reactive action identity.
    #[must_use]
    pub fn symbol(&self) -> &str {
        &self.symbol
    }
    /// Closed operation class.
    #[must_use]
    pub const fn kind(&self) -> OperationKind {
        self.kind
    }
    /// Exact immutable module hash when applicable.
    #[must_use]
    pub const fn module_hash(&self) -> Option<[u8; 32]> {
        self.module_hash
    }
    /// Exact compiler plan hash when applicable.
    #[must_use]
    pub const fn plan_hash(&self) -> Option<[u8; 32]> {
        self.plan_hash
    }
    /// Canonical generated input-schema hash.
    #[must_use]
    pub fn input_schema_hash(&self) -> String {
        hex(&self.input_schema_hash)
    }
    /// Canonical generated result-schema hash.
    #[must_use]
    pub fn result_schema_hash(&self) -> String {
        hex(&self.result_schema_hash)
    }
    /// Closed reactive operation class when this is reactive.
    #[must_use]
    pub const fn reactive_kind(&self) -> Option<ReactiveKind> {
        self.reactive_kind
    }
    /// Exact generated reactive action when this is reactive.
    #[must_use]
    pub fn reactive_action(&self) -> Option<&str> {
        self.reactive_action.as_deref()
    }
    /// Exact contextual reaction target when this is a reaction action.
    #[must_use]
    pub const fn reaction(&self) -> Option<&ReactionSpec> {
        self.reaction.as_ref()
    }
}

/// Exact application-only dispatch catalog.
#[derive(Clone, Debug)]
pub struct ApplicationCatalog {
    application_manifest_hash: [u8; 32],
    catalog_hash: [u8; 32],
    database: String,
    role: String,
    role_definition_hash: [u8; 32],
    contract_lineage: String,
    contract_version: u64,
    contract_bundle_hash: [u8; 32],
    operations: BTreeMap<String, OperationSpec>,
}

impl ApplicationCatalog {
    /// Loads and cross-checks one exact lock, manifest, and generated catalog.
    pub fn from_exact_artifacts(
        lock: &[u8],
        manifest: &[u8],
        tools: &[u8],
        database: &str,
        role: &str,
    ) -> Result<Self, CatalogError> {
        if lock.is_empty()
            || manifest.is_empty()
            || tools.is_empty()
            || lock.len() > MAX_ARTIFACT_BYTES
            || manifest.len() > MAX_ARTIFACT_BYTES
            || tools.len() > MAX_ARTIFACT_BYTES
            || database.is_empty()
            || database.len() > 64
            || role.is_empty()
            || role.len() > 256
        {
            return Err(CatalogError::InvalidArtifact);
        }
        let manifest_file_hash = *hash_generated_artifact(manifest).as_bytes();
        let tools_file_hash = *hash_generated_artifact(tools).as_bytes();
        let lock: ExactLock =
            serde_json::from_slice(lock).map_err(|_| CatalogError::InvalidArtifact)?;
        let manifest: ExactManifest =
            serde_json::from_slice(manifest).map_err(|_| CatalogError::InvalidArtifact)?;
        let tools_value: Value =
            serde_json::from_slice(tools).map_err(|_| CatalogError::InvalidArtifact)?;
        let tools: GeneratedCatalog = serde_json::from_value(tools_value.clone())
            .map_err(|_| CatalogError::InvalidArtifact)?;
        if !lock.artifacts.iter().any(|artifact| {
            artifact.kind == "mcp"
                && parse_hash(&artifact.content_hash).ok() == Some(tools_file_hash)
        }) || !lock.artifacts.iter().any(|artifact| {
            artifact.kind == "manifest"
                && parse_hash(&artifact.content_hash).ok() == Some(manifest_file_hash)
        }) {
            return Err(CatalogError::IdentityMismatch);
        }
        if !matches!(
            lock.schema.as_str(),
            "riffdb.application-lock/v3"
                | "riffdb.application-lock/v5"
                | "riffdb.application-lock/v6"
                | "riffdb.application-lock/v7"
        ) || !matches!(
            manifest.schema.as_str(),
            "riffdb.application-manifest/v1"
                | "riffdb.application-manifest/v2"
                | "riffdb.application-manifest/v3"
                | "riffdb.application-manifest/v4"
        ) || !matches!(
            tools.schema.as_str(),
            "riffdb-generated-application-operations/v2"
                | "riffdb-generated-application-operations/v3"
        ) || (tools.schema == "riffdb-generated-application-operations/v2"
            && !tools.sdk_tools.is_empty())
            || (tools.schema == "riffdb-generated-application-operations/v3"
                && tools.sdk_tools.is_empty())
            || lock.exact_manifest_hash != tools.application_manifest_hash
            || manifest.contract.lineage != lock.contract.lineage
            || manifest.contract.version != lock.contract.version
            || manifest.contract.bundle_hash != lock.contract.bundle_hash
        {
            return Err(CatalogError::IdentityMismatch);
        }
        let selected_role = lock
            .roles
            .iter()
            .find(|candidate| candidate.definition.name == role)
            .ok_or(CatalogError::IdentityMismatch)?;
        if !manifest
            .roles
            .iter()
            .any(|candidate| candidate.name == role)
        {
            return Err(CatalogError::IdentityMismatch);
        }
        let role_definition_hash = parse_hash(&selected_role.definition_hash)?;
        let manifest_hash = parse_hash(&lock.exact_manifest_hash)?;
        let bundle_hash = parse_hash(&lock.contract.bundle_hash)?;
        let mut commands = BTreeMap::new();
        for command in &selected_role.definition.commands {
            let plan = parse_hash(&command.plan_hash)?;
            if commands.insert(plan, command.name.clone()).is_some() {
                return Err(CatalogError::IdentityMismatch);
            }
        }
        let mut available_queries = BTreeMap::new();
        for module in &lock.modules {
            let module_hash = parse_hash(&module.module_hash)?;
            for query in &module.queries {
                if available_queries
                    .insert(
                        (module_hash, query.name.clone()),
                        parse_hash(&query.plan_hash)?,
                    )
                    .is_some()
                {
                    return Err(CatalogError::IdentityMismatch);
                }
            }
        }
        let mut queries = BTreeMap::new();
        for query in &selected_role.definition.queries {
            let identity = (parse_hash(&query.module_hash)?, query.name.clone());
            let plan_hash = parse_hash(&query.plan_hash)?;
            if available_queries.get(&identity) != Some(&plan_hash)
                || queries.insert(identity, plan_hash).is_some()
            {
                return Err(CatalogError::IdentityMismatch);
            }
        }
        let mut allowed_reactive = BTreeSet::new();
        for operation in &selected_role.definition.event_streams {
            allowed_reactive.insert((
                parse_hash(&operation.module_hash)?,
                operation.name.clone(),
                ReactiveKind::Stream,
            ));
        }
        for operation in &selected_role.definition.watch_queries {
            allowed_reactive.insert((
                parse_hash(&operation.module_hash)?,
                operation.name.clone(),
                ReactiveKind::Watch,
            ));
        }
        for operation in &selected_role.definition.agent_subscriptions {
            allowed_reactive.insert((
                parse_hash(&operation.module_hash)?,
                operation.name.clone(),
                ReactiveKind::Subscription,
            ));
        }
        let mut operations = BTreeMap::new();
        let mut found_commands = BTreeSet::new();
        for command in tools.commands {
            if parse_hash(&command.contract_bundle_hash)? != bundle_hash {
                return Err(CatalogError::IdentityMismatch);
            }
            let plan_hash = parse_hash(&command.plan_hash)?;
            let Some(symbol) = commands.get(&plan_hash).cloned() else {
                continue;
            };
            if symbol != command.operation_name {
                return Err(CatalogError::IdentityMismatch);
            }
            found_commands.insert(plan_hash);
            insert_operation(
                &mut operations,
                OperationArtifact {
                    public_name: command.name,
                    symbol: command.operation_name,
                    kind: OperationKind::Command,
                    module_hash: None,
                    plan_hash: Some(plan_hash),
                    input_schema: command.input_schema,
                    result_schema: command.result_schema,
                    reactive_kind: None,
                    reactive_action: None,
                    reaction: None,
                },
            )?;
        }
        let mut found_queries = BTreeSet::new();
        let mut catalog_queries = BTreeSet::new();
        for query in tools.tools.into_iter().chain(tools.sdk_tools) {
            let module_hash = parse_hash(&query.module_hash)?;
            let query_identity = (module_hash, query.operation_name.clone());
            if !catalog_queries.insert(query_identity.clone()) {
                return Err(CatalogError::DuplicateOperation);
            }
            let Some(plan_hash) = queries.get(&query_identity).copied() else {
                continue;
            };
            found_queries.insert(query_identity);
            insert_operation(
                &mut operations,
                OperationArtifact {
                    public_name: query.name,
                    symbol: query.operation_name,
                    kind: OperationKind::Query,
                    module_hash: Some(module_hash),
                    plan_hash: Some(plan_hash),
                    input_schema: query.input_schema,
                    result_schema: query.result_schema,
                    reactive_kind: None,
                    reactive_action: None,
                    reaction: None,
                },
            )?;
        }
        let mut found_reactive = BTreeSet::new();
        for reactive in tools.reactive_tools {
            let module_hash = parse_hash(&reactive.reactive_module_hash)?;
            if !manifest
                .reactive_modules
                .iter()
                .any(|module| module.module_hash == reactive.reactive_module_hash)
            {
                return Err(CatalogError::IdentityMismatch);
            }
            let reactive_kind = match reactive.operation_kind.as_str() {
                "stream" => ReactiveKind::Stream,
                "watch" => ReactiveKind::Watch,
                "subscription" => ReactiveKind::Subscription,
                _ => return Err(CatalogError::InvalidArtifact),
            };
            let reactive_identity = (module_hash, reactive.operation_name.clone(), reactive_kind);
            if !allowed_reactive.contains(&reactive_identity) {
                continue;
            }
            found_reactive.insert(reactive_identity);
            insert_operation(
                &mut operations,
                OperationArtifact {
                    public_name: reactive.name,
                    symbol: reactive.operation_name,
                    kind: OperationKind::Reactive,
                    module_hash: Some(module_hash),
                    plan_hash: None,
                    input_schema: reactive.input_schema,
                    result_schema: reactive.result_schema,
                    reactive_kind: Some(reactive_kind),
                    reactive_action: Some(reactive.action),
                    reaction: match (
                        reactive.reaction_name,
                        reactive.reaction_command_name,
                        reactive.reaction_command_id,
                    ) {
                        (Some(name), Some(command_name), Some(command_id)) if command_id != 0 => {
                            Some(ReactionSpec {
                                name,
                                command_name,
                                command_id,
                            })
                        }
                        (None, None, None) => None,
                        _ => return Err(CatalogError::InvalidArtifact),
                    },
                },
            )?;
        }
        if found_commands.len() != commands.len()
            || found_queries.len() != queries.len()
            || found_reactive.len() != allowed_reactive.len()
            || operations.is_empty()
            || operations.len() > MAX_OPERATIONS
        {
            return Err(CatalogError::InvalidArtifact);
        }
        let catalog_hash = tools_file_hash;
        Ok(Self {
            application_manifest_hash: manifest_hash,
            catalog_hash,
            database: database.to_owned(),
            role: role.to_owned(),
            role_definition_hash,
            contract_lineage: lock.contract.lineage,
            contract_version: lock.contract.version,
            contract_bundle_hash: bundle_hash,
            operations,
        })
    }

    /// Finds only a generated exact application operation.
    #[must_use]
    pub fn operation(&self, name: &str) -> Option<&OperationSpec> {
        self.operations.get(name)
    }
    /// Exact application manifest identity.
    #[must_use]
    pub fn application_manifest_hash(&self) -> String {
        hex(&self.application_manifest_hash)
    }
    /// Exact canonical generated catalog identity.
    #[must_use]
    pub fn catalog_hash(&self) -> String {
        hex(&self.catalog_hash)
    }
    /// Host-selected database alias.
    #[must_use]
    pub fn database(&self) -> &str {
        &self.database
    }
    /// Exact symbolic application role selected for this host.
    #[must_use]
    pub fn role(&self) -> &str {
        &self.role
    }
    /// Exact selected role-definition identity.
    #[must_use]
    pub fn role_definition_hash(&self) -> String {
        hex(&self.role_definition_hash)
    }
    /// Exact contract lineage.
    #[must_use]
    pub fn contract_lineage(&self) -> &str {
        &self.contract_lineage
    }
    /// Exact contract version.
    #[must_use]
    pub const fn contract_version(&self) -> u64 {
        self.contract_version
    }
    /// Exact bundle hash.
    #[must_use]
    pub const fn contract_bundle_hash(&self) -> [u8; 32] {
        self.contract_bundle_hash
    }
    /// Exact bundle hash in canonical lowercase hexadecimal form.
    #[must_use]
    pub fn contract_bundle_hash_hex(&self) -> String {
        hex(&self.contract_bundle_hash)
    }
}

/// Safe closed catalog loading failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CatalogError {
    /// Artifact shape, size, encoding, or hash text is invalid.
    InvalidArtifact,
    /// Exact lock, manifest, role, module, or operation identities disagree.
    IdentityMismatch,
    /// Two generated operations claim one local name.
    DuplicateOperation,
}

impl fmt::Display for CatalogError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::InvalidArtifact => "driver application artifact is invalid",
            Self::IdentityMismatch => "driver application identities do not match",
            Self::DuplicateOperation => "driver operation catalog contains a duplicate",
        })
    }
}
impl std::error::Error for CatalogError {}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ExactLock {
    schema: String,
    exact_manifest_hash: String,
    contract: LockContract,
    modules: Vec<LockModule>,
    roles: Vec<LockRole>,
    artifacts: Vec<LockArtifact>,
    #[serde(default, rename = "migrations")]
    _migrations: Value,
    #[serde(default, rename = "reactive_modules")]
    _reactive_modules: Value,
    #[serde(default, rename = "compiler_formats")]
    _compiler_formats: Value,
    #[serde(default, rename = "source_hash")]
    _source_hash: String,
}
#[derive(Deserialize)]
struct LockArtifact {
    kind: String,
    content_hash: String,
}
#[derive(Deserialize)]
struct LockContract {
    lineage: String,
    version: u64,
    bundle_hash: String,
}
#[derive(Deserialize)]
struct LockModule {
    #[serde(rename = "name")]
    _name: String,
    module_hash: String,
    queries: Vec<LockQuery>,
}
#[derive(Deserialize)]
struct LockQuery {
    name: String,
    plan_hash: String,
}
#[derive(Deserialize)]
struct LockRole {
    definition_hash: String,
    definition: LockRoleDefinition,
}
#[derive(Deserialize)]
struct LockRoleDefinition {
    name: String,
    #[serde(default)]
    commands: Vec<LockCommand>,
    #[serde(default)]
    queries: Vec<LockRoleQuery>,
    #[serde(default)]
    event_streams: Vec<LockReactiveOperation>,
    #[serde(default)]
    watch_queries: Vec<LockReactiveOperation>,
    #[serde(default)]
    agent_subscriptions: Vec<LockReactiveOperation>,
}
#[derive(Deserialize)]
struct LockCommand {
    name: String,
    plan_hash: String,
}
#[derive(Deserialize)]
struct LockRoleQuery {
    name: String,
    module_hash: String,
    plan_hash: String,
}
#[derive(Deserialize)]
struct LockReactiveOperation {
    name: String,
    module_hash: String,
    #[serde(rename = "operation_hash")]
    _operation_hash: String,
}
#[derive(Deserialize)]
struct ExactManifest {
    schema: String,
    contract: ManifestContract,
    roles: Vec<ManifestRole>,
    #[serde(default)]
    reactive_modules: Vec<ManifestReactiveModule>,
}
#[derive(Deserialize)]
struct ManifestRole {
    name: String,
}
#[derive(Deserialize)]
struct ManifestContract {
    lineage: String,
    version: u64,
    bundle_hash: String,
}
#[derive(Deserialize)]
struct ManifestReactiveModule {
    module_hash: String,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct GeneratedCatalog {
    schema: String,
    application_manifest_hash: String,
    tools: Vec<GeneratedTool>,
    #[serde(default)]
    sdk_tools: Vec<GeneratedTool>,
    commands: Vec<GeneratedCommand>,
    reactive_tools: Vec<GeneratedReactiveTool>,
}
#[derive(Deserialize)]
struct GeneratedTool {
    name: String,
    operation_name: String,
    module_hash: String,
    input_schema: Value,
    result_schema: Value,
}
#[derive(Deserialize)]
struct GeneratedCommand {
    name: String,
    operation_name: String,
    contract_bundle_hash: String,
    plan_hash: String,
    input_schema: Value,
    result_schema: Value,
}
#[derive(Deserialize)]
struct GeneratedReactiveTool {
    name: String,
    operation_name: String,
    action: String,
    operation_kind: String,
    reaction_name: Option<String>,
    reaction_command_name: Option<String>,
    reaction_command_id: Option<u32>,
    reactive_module_hash: String,
    input_schema: Value,
    result_schema: Value,
}

struct OperationArtifact {
    public_name: String,
    symbol: String,
    kind: OperationKind,
    module_hash: Option<[u8; 32]>,
    plan_hash: Option<[u8; 32]>,
    input_schema: Value,
    result_schema: Value,
    reactive_kind: Option<ReactiveKind>,
    reactive_action: Option<String>,
    reaction: Option<ReactionSpec>,
}

fn insert_operation(
    operations: &mut BTreeMap<String, OperationSpec>,
    operation: OperationArtifact,
) -> Result<(), CatalogError> {
    let OperationArtifact {
        public_name,
        symbol,
        kind,
        module_hash,
        plan_hash,
        input_schema,
        result_schema,
        reactive_kind,
        reactive_action,
        reaction,
    } = operation;
    if public_name.is_empty()
        || public_name.len() > 256
        || symbol.is_empty()
        || symbol.len() > 256
        || !public_name
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
    {
        return Err(CatalogError::InvalidArtifact);
    }
    let spec = OperationSpec {
        public_name: public_name.clone(),
        symbol,
        kind,
        module_hash,
        plan_hash,
        input_schema_hash: canonical_hash(&input_schema)?,
        result_schema_hash: canonical_hash(&result_schema)?,
        reactive_kind,
        reactive_action,
        reaction,
    };
    if operations.insert(public_name, spec).is_some() {
        return Err(CatalogError::DuplicateOperation);
    }
    Ok(())
}

fn canonical_hash(value: &Value) -> Result<[u8; 32], CatalogError> {
    let bytes = serde_json::to_vec(value).map_err(|_| CatalogError::InvalidArtifact)?;
    Ok(Sha256::digest(bytes).into())
}
fn parse_hash(value: &str) -> Result<[u8; 32], CatalogError> {
    if value.len() != 64 {
        return Err(CatalogError::InvalidArtifact);
    }
    let mut out = [0; 32];
    for (index, pair) in value.as_bytes().chunks_exact(2).enumerate() {
        out[index] = (hex_nibble(pair[0])? << 4) | hex_nibble(pair[1])?;
    }
    Ok(out)
}
fn hex_nibble(value: u8) -> Result<u8, CatalogError> {
    match value {
        b'0'..=b'9' => Ok(value - b'0'),
        b'a'..=b'f' => Ok(value - b'a' + 10),
        _ => Err(CatalogError::InvalidArtifact),
    }
}
fn hex(bytes: &[u8; 32]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut value = String::with_capacity(64);
    for byte in bytes {
        value.push(char::from(HEX[usize::from(byte >> 4)]));
        value.push(char::from(HEX[usize::from(byte & 0xf)]));
    }
    value
}
