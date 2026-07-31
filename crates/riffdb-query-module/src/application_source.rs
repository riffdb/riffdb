//! Author-owned symbolic application source manifest.

use std::collections::BTreeSet;
use std::fmt;

use riffdb_contract_ir::ContractBundle;
use riffdb_types::{ApplicationSourceHash, hash_application_source};
use serde_json::{Map, Value, json};

use crate::{ApplicationManifest, QueryModule};

/// Symbolic application-source schema identifier.
pub const APPLICATION_SOURCE_SCHEMA_V1: &str = "riffdb.application-source/v1";
/// Symbolic application-source schema with a required Python target.
pub const APPLICATION_SOURCE_SCHEMA_V2: &str = "riffdb.application-source/v2";
/// Maximum accepted application-source bytes.
pub const MAX_APPLICATION_SOURCE_BYTES: usize = 1_048_576;
const MAX_NAME_BYTES: usize = 256;
const MAX_PATH_BYTES: usize = 512;
const MAX_QUERY_MODULES: usize = 32;
const MAX_QUERY_SOURCES: usize = 4_096;
const MAX_ROLES: usize = 128;
const MAX_ROLE_OPERATIONS: usize = 4_096;
const MAX_SEED_INPUTS: usize = 256;

/// Symbolic contract source selected by an application author.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ApplicationSourceContract {
    source: String,
    lineage: String,
    version: u64,
}

impl ApplicationSourceContract {
    /// Workspace-relative contract source.
    #[must_use]
    pub fn source(&self) -> &str {
        &self.source
    }

    /// Declared contract lineage.
    #[must_use]
    pub fn lineage(&self) -> &str {
        &self.lineage
    }

    /// Declared positive contract version.
    #[must_use]
    pub const fn version(&self) -> u64 {
        self.version
    }
}

/// One symbolic named RiffQL source.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ApplicationSourceQuery {
    name: String,
    source: String,
}

impl ApplicationSourceQuery {
    /// Declared query name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Workspace-relative RiffQL source.
    #[must_use]
    pub fn source(&self) -> &str {
        &self.source
    }
}

/// One symbolic query-module source group.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ApplicationSourceQueryModule {
    name: String,
    version: u64,
    queries: Vec<ApplicationSourceQuery>,
}

impl ApplicationSourceQueryModule {
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

    /// Query sources in canonical name order.
    #[must_use]
    pub fn queries(&self) -> &[ApplicationSourceQuery] {
        &self.queries
    }
}

/// Symbolic tenant scope declared by a source role.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ApplicationSourceTenantScope {
    /// The role is global.
    Global,
    /// Binding requires one concrete tenant.
    Tenant,
}

impl ApplicationSourceTenantScope {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Global => "global",
            Self::Tenant => "tenant",
        }
    }
}

/// One author-owned symbolic role.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ApplicationSourceRole {
    name: String,
    environment: String,
    tenant_scope: ApplicationSourceTenantScope,
    queries: Vec<String>,
    commands: Vec<String>,
}

impl ApplicationSourceRole {
    /// Role name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Declared environment.
    #[must_use]
    pub fn environment(&self) -> &str {
        &self.environment
    }

    /// Declared tenant scope.
    #[must_use]
    pub const fn tenant_scope(&self) -> ApplicationSourceTenantScope {
        self.tenant_scope
    }

    /// Named-query allowlist.
    #[must_use]
    pub fn queries(&self) -> &[String] {
        &self.queries
    }

    /// Symbolic-command allowlist.
    #[must_use]
    pub fn commands(&self) -> &[String] {
        &self.commands
    }
}

/// Compiler-generated output paths.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ApplicationSourceGeneration {
    rust: String,
    typescript: String,
    mcp: String,
    python: Option<String>,
}

impl ApplicationSourceGeneration {
    /// Rust output path.
    #[must_use]
    pub fn rust(&self) -> &str {
        &self.rust
    }

    /// TypeScript output path.
    #[must_use]
    pub fn typescript(&self) -> &str {
        &self.typescript
    }

    /// MCP output path.
    #[must_use]
    pub fn mcp(&self) -> &str {
        &self.mcp
    }

    /// Python output path for V2 source manifests.
    #[must_use]
    pub fn python(&self) -> Option<&str> {
        self.python.as_deref()
    }
}

/// Canonical author-owned symbolic application source.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ApplicationSourceManifest {
    schema: &'static str,
    application_name: String,
    contract: ApplicationSourceContract,
    query_modules: Vec<ApplicationSourceQueryModule>,
    roles: Vec<ApplicationSourceRole>,
    generation: ApplicationSourceGeneration,
    seed_inputs: Vec<String>,
    canonical_bytes: Vec<u8>,
    identity: ApplicationSourceHash,
}

impl ApplicationSourceManifest {
    /// Parses, validates, sorts, and canonically encodes symbolic source.
    pub fn parse(source: &str) -> Result<Self, ApplicationSourceError> {
        if source.is_empty() || source.len() > MAX_APPLICATION_SOURCE_BYTES {
            return Err(ApplicationSourceError::new(
                ApplicationSourceErrorKind::LimitExceeded,
            ));
        }
        let value: Value = serde_json::from_str(source)
            .map_err(|_| ApplicationSourceError::new(ApplicationSourceErrorKind::InvalidJson))?;
        let root = object(
            &value,
            &[
                "application",
                "contract",
                "generation",
                "query_modules",
                "roles",
                "schema",
                "seed_inputs",
            ],
        )?;
        let schema = match string(root, "schema")? {
            APPLICATION_SOURCE_SCHEMA_V1 => APPLICATION_SOURCE_SCHEMA_V1,
            APPLICATION_SOURCE_SCHEMA_V2 => APPLICATION_SOURCE_SCHEMA_V2,
            _ => {
                return Err(ApplicationSourceError::new(
                    ApplicationSourceErrorKind::UnsupportedVersion,
                ));
            }
        };
        let application_name = checked_name(string(root, "application")?)?;
        let contract = parse_contract(required(root, "contract")?)?;
        let query_modules = parse_modules(required(root, "query_modules")?)?;
        let roles = parse_roles(required(root, "roles")?, &query_modules)?;
        let generation = parse_generation(required(root, "generation")?, schema)?;
        let seed_inputs = parse_paths(required(root, "seed_inputs")?, MAX_SEED_INPUTS)?;
        let canonical_value = canonical_value(
            &application_name,
            &contract,
            &query_modules,
            &roles,
            &generation,
            &seed_inputs,
            schema,
        );
        let mut canonical_bytes = serde_json::to_vec(&canonical_value)
            .map_err(|_| ApplicationSourceError::new(ApplicationSourceErrorKind::InvalidJson))?;
        canonical_bytes.push(b'\n');
        if canonical_bytes.len() > MAX_APPLICATION_SOURCE_BYTES {
            return Err(ApplicationSourceError::new(
                ApplicationSourceErrorKind::LimitExceeded,
            ));
        }
        let identity = hash_application_source(&canonical_bytes);
        Ok(Self {
            schema,
            application_name,
            contract,
            query_modules,
            roles,
            generation,
            seed_inputs,
            canonical_bytes,
            identity,
        })
    }

    /// Strictly decodes canonical source bytes.
    pub fn decode_canonical(bytes: &[u8]) -> Result<Self, ApplicationSourceError> {
        let source = std::str::from_utf8(bytes)
            .map_err(|_| ApplicationSourceError::new(ApplicationSourceErrorKind::InvalidJson))?;
        let manifest = Self::parse(source)?;
        if manifest.canonical_bytes != bytes {
            return Err(ApplicationSourceError::new(
                ApplicationSourceErrorKind::NonCanonical,
            ));
        }
        Ok(manifest)
    }

    /// Application package name.
    #[must_use]
    pub fn application_name(&self) -> &str {
        &self.application_name
    }

    /// Exact symbolic source schema identifier.
    #[must_use]
    pub const fn schema(&self) -> &'static str {
        self.schema
    }

    /// Symbolic contract declaration.
    #[must_use]
    pub const fn contract(&self) -> &ApplicationSourceContract {
        &self.contract
    }

    /// Query modules in canonical name order.
    #[must_use]
    pub fn query_modules(&self) -> &[ApplicationSourceQueryModule] {
        &self.query_modules
    }

    /// Symbolic roles in canonical name order.
    #[must_use]
    pub fn roles(&self) -> &[ApplicationSourceRole] {
        &self.roles
    }

    /// Generated output targets.
    #[must_use]
    pub const fn generation(&self) -> &ApplicationSourceGeneration {
        &self.generation
    }

    /// Seed input paths in canonical order.
    #[must_use]
    pub fn seed_inputs(&self) -> &[String] {
        &self.seed_inputs
    }

    /// Canonical source bytes.
    #[must_use]
    pub fn canonical_bytes(&self) -> &[u8] {
        &self.canonical_bytes
    }

    /// Domain-separated source-manifest identity.
    #[must_use]
    pub const fn identity(&self) -> ApplicationSourceHash {
        self.identity
    }

    /// Produces the compatible exact V1 manifest after every compiled identity
    /// is known. This operation performs no I/O or deployment.
    pub fn exact_manifest(
        &self,
        contract: &ContractBundle,
        modules: &[QueryModule],
    ) -> Result<ApplicationManifest, ApplicationSourceError> {
        if contract.lineage().as_str() != self.contract.lineage
            || contract.contract_version().get() != self.contract.version
            || modules.len() != self.query_modules.len()
        {
            return Err(ApplicationSourceError::new(
                ApplicationSourceErrorKind::IdentityMismatch,
            ));
        }
        let mut exact_modules = Vec::with_capacity(modules.len());
        for declared in &self.query_modules {
            let module = modules
                .iter()
                .find(|module| module.name().as_str() == declared.name)
                .ok_or_else(|| {
                    ApplicationSourceError::new(ApplicationSourceErrorKind::IdentityMismatch)
                })?;
            if module.version().get() != declared.version
                || module.contract_hash() != contract.bundle_hash()
                || module.queries().len() != declared.queries.len()
                || declared
                    .queries
                    .iter()
                    .any(|query| module.query(&query.name).is_none())
            {
                return Err(ApplicationSourceError::new(
                    ApplicationSourceErrorKind::IdentityMismatch,
                ));
            }
            exact_modules.push(json!({
                "module_hash": hex(module.identity().as_bytes()),
                "name": declared.name,
                "queries": declared.queries.iter().map(|query| json!({
                    "name": query.name,
                    "source": query.source,
                })).collect::<Vec<_>>(),
                "version": declared.version,
            }));
        }
        let value = json!({
            "application": self.application_name,
            "contract": {
                "bundle_hash": hex(contract.bundle_hash().as_bytes()),
                "lineage": self.contract.lineage,
                "source": self.contract.source,
                "version": self.contract.version,
            },
            "generation": {
                "mcp": self.generation.mcp,
                "rust": self.generation.rust,
                "typescript": self.generation.typescript,
            },
            "query_modules": exact_modules,
            "roles": self.roles.iter().map(|role| json!({
                "commands": role.commands,
                "environment": role.environment,
                "name": role.name,
                "queries": role.queries,
                "tenant_scope": role.tenant_scope.as_str(),
            })).collect::<Vec<_>>(),
            "schema": crate::APPLICATION_MANIFEST_SCHEMA_V1,
            "seed_inputs": self.seed_inputs,
        });
        let source = serde_json::to_string(&value)
            .map_err(|_| ApplicationSourceError::new(ApplicationSourceErrorKind::InvalidJson))?;
        ApplicationManifest::parse(&source)
            .map_err(|_| ApplicationSourceError::new(ApplicationSourceErrorKind::IdentityMismatch))
    }
}

/// Closed symbolic source failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ApplicationSourceErrorKind {
    /// Invalid JSON.
    InvalidJson,
    /// Unsupported schema.
    UnsupportedVersion,
    /// Missing, extra, or wrongly typed member.
    InvalidShape,
    /// Invalid symbolic name.
    InvalidName,
    /// Invalid workspace-relative path.
    InvalidPath,
    /// Duplicate declaration.
    Duplicate,
    /// Role names an unknown query.
    UnknownOperation,
    /// Hard source bound exceeded.
    LimitExceeded,
    /// Strict decoder observed noncanonical bytes.
    NonCanonical,
    /// Compiled contract/module identity does not match symbolic declarations.
    IdentityMismatch,
}

/// Bounded symbolic source error.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ApplicationSourceError {
    kind: ApplicationSourceErrorKind,
}

impl ApplicationSourceError {
    const fn new(kind: ApplicationSourceErrorKind) -> Self {
        Self { kind }
    }

    /// Closed failure kind.
    #[must_use]
    pub const fn kind(self) -> ApplicationSourceErrorKind {
        self.kind
    }
}

impl fmt::Display for ApplicationSourceError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self.kind {
            ApplicationSourceErrorKind::InvalidJson => "application source JSON is invalid",
            ApplicationSourceErrorKind::UnsupportedVersion => {
                "application source version is unsupported"
            }
            ApplicationSourceErrorKind::InvalidShape => "application source shape is invalid",
            ApplicationSourceErrorKind::InvalidName => "application source name is invalid",
            ApplicationSourceErrorKind::InvalidPath => "application source path is invalid",
            ApplicationSourceErrorKind::Duplicate => {
                "application source contains a duplicate declaration"
            }
            ApplicationSourceErrorKind::UnknownOperation => {
                "application source role names an unknown query"
            }
            ApplicationSourceErrorKind::LimitExceeded => "application source limit exceeded",
            ApplicationSourceErrorKind::NonCanonical => {
                "application source bytes are not canonical"
            }
            ApplicationSourceErrorKind::IdentityMismatch => {
                "compiled application does not match symbolic source"
            }
        })
    }
}

impl std::error::Error for ApplicationSourceError {}

fn canonical_value(
    application: &str,
    contract: &ApplicationSourceContract,
    modules: &[ApplicationSourceQueryModule],
    roles: &[ApplicationSourceRole],
    generation: &ApplicationSourceGeneration,
    seeds: &[String],
    schema: &'static str,
) -> Value {
    let mut generation_value = Map::new();
    generation_value.insert("mcp".to_owned(), json!(generation.mcp));
    if let Some(python) = &generation.python {
        generation_value.insert("python".to_owned(), json!(python));
    }
    generation_value.insert("rust".to_owned(), json!(generation.rust));
    generation_value.insert("typescript".to_owned(), json!(generation.typescript));
    json!({
        "application": application,
        "contract": {
            "lineage": contract.lineage,
            "source": contract.source,
            "version": contract.version,
        },
        "generation": generation_value,
        "query_modules": modules.iter().map(|module| json!({
            "name": module.name,
            "queries": module.queries.iter().map(|query| json!({
                "name": query.name,
                "source": query.source,
            })).collect::<Vec<_>>(),
            "version": module.version,
        })).collect::<Vec<_>>(),
        "roles": roles.iter().map(|role| json!({
            "commands": role.commands,
            "environment": role.environment,
            "name": role.name,
            "queries": role.queries,
            "tenant_scope": role.tenant_scope.as_str(),
        })).collect::<Vec<_>>(),
        "schema": schema,
        "seed_inputs": seeds,
    })
}

fn parse_contract(value: &Value) -> Result<ApplicationSourceContract, ApplicationSourceError> {
    let object = object(value, &["lineage", "source", "version"])?;
    Ok(ApplicationSourceContract {
        source: checked_path(string(object, "source")?)?,
        lineage: checked_name(string(object, "lineage")?)?,
        version: positive_u64(object, "version")?,
    })
}

fn parse_modules(
    value: &Value,
) -> Result<Vec<ApplicationSourceQueryModule>, ApplicationSourceError> {
    let values = array(value, 1, MAX_QUERY_MODULES)?;
    let mut modules = Vec::with_capacity(values.len());
    for value in values {
        let module_object = object(value, &["name", "queries", "version"])?;
        let query_values = array(required(module_object, "queries")?, 1, MAX_QUERY_SOURCES)?;
        let mut queries = Vec::with_capacity(query_values.len());
        for query in query_values {
            let query = object(query, &["name", "source"])?;
            queries.push(ApplicationSourceQuery {
                name: checked_name(string(query, "name")?)?,
                source: checked_path(string(query, "source")?)?,
            });
        }
        queries.sort_by(|left, right| left.name.cmp(&right.name));
        ensure_unique(queries.iter().map(|query| query.name.as_str()))?;
        ensure_unique(queries.iter().map(|query| query.source.as_str()))?;
        modules.push(ApplicationSourceQueryModule {
            name: checked_name(string(module_object, "name")?)?,
            version: positive_u64(module_object, "version")?,
            queries,
        });
    }
    modules.sort_by(|left, right| left.name.cmp(&right.name));
    ensure_unique(modules.iter().map(|module| module.name.as_str()))?;
    Ok(modules)
}

fn parse_roles(
    value: &Value,
    modules: &[ApplicationSourceQueryModule],
) -> Result<Vec<ApplicationSourceRole>, ApplicationSourceError> {
    let values = array(value, 1, MAX_ROLES)?;
    let available = modules
        .iter()
        .flat_map(|module| module.queries.iter().map(|query| query.name.as_str()))
        .collect::<BTreeSet<_>>();
    let mut roles = Vec::with_capacity(values.len());
    for value in values {
        let object = object(
            value,
            &["commands", "environment", "name", "queries", "tenant_scope"],
        )?;
        let mut queries = parse_names(required(object, "queries")?, MAX_ROLE_OPERATIONS)?;
        let mut commands = parse_names(required(object, "commands")?, MAX_ROLE_OPERATIONS)?;
        queries.sort();
        commands.sort();
        ensure_unique(queries.iter().map(String::as_str))?;
        ensure_unique(commands.iter().map(String::as_str))?;
        if queries
            .iter()
            .any(|query| !available.contains(query.as_str()))
        {
            return Err(ApplicationSourceError::new(
                ApplicationSourceErrorKind::UnknownOperation,
            ));
        }
        roles.push(ApplicationSourceRole {
            name: checked_name(string(object, "name")?)?,
            environment: checked_name(string(object, "environment")?)?,
            tenant_scope: match string(object, "tenant_scope")? {
                "global" => ApplicationSourceTenantScope::Global,
                "tenant" => ApplicationSourceTenantScope::Tenant,
                _ => {
                    return Err(ApplicationSourceError::new(
                        ApplicationSourceErrorKind::InvalidShape,
                    ));
                }
            },
            queries,
            commands,
        });
    }
    roles.sort_by(|left, right| left.name.cmp(&right.name));
    ensure_unique(roles.iter().map(|role| role.name.as_str()))?;
    Ok(roles)
}

fn parse_generation(
    value: &Value,
    schema: &str,
) -> Result<ApplicationSourceGeneration, ApplicationSourceError> {
    let object = if schema == APPLICATION_SOURCE_SCHEMA_V1 {
        object(value, &["mcp", "rust", "typescript"])?
    } else {
        object(value, &["mcp", "python", "rust", "typescript"])?
    };
    let generation = ApplicationSourceGeneration {
        rust: checked_path(string(object, "rust")?)?,
        typescript: checked_path(string(object, "typescript")?)?,
        mcp: checked_path(string(object, "mcp")?)?,
        python: if schema == APPLICATION_SOURCE_SCHEMA_V2 {
            Some(checked_path(string(object, "python")?)?)
        } else {
            None
        },
    };
    let mut paths = vec![
        generation.rust.as_str(),
        generation.typescript.as_str(),
        generation.mcp.as_str(),
    ];
    if let Some(python) = generation.python.as_deref() {
        paths.push(python);
    }
    ensure_unique(paths)?;
    Ok(generation)
}

fn parse_paths(value: &Value, maximum: usize) -> Result<Vec<String>, ApplicationSourceError> {
    let mut paths = array(value, 0, maximum)?
        .iter()
        .map(|value| {
            value
                .as_str()
                .ok_or_else(|| {
                    ApplicationSourceError::new(ApplicationSourceErrorKind::InvalidShape)
                })
                .and_then(checked_path)
        })
        .collect::<Result<Vec<_>, _>>()?;
    paths.sort();
    ensure_unique(paths.iter().map(String::as_str))?;
    Ok(paths)
}

fn parse_names(value: &Value, maximum: usize) -> Result<Vec<String>, ApplicationSourceError> {
    array(value, 0, maximum)?
        .iter()
        .map(|value| {
            value
                .as_str()
                .ok_or_else(|| {
                    ApplicationSourceError::new(ApplicationSourceErrorKind::InvalidShape)
                })
                .and_then(checked_name)
        })
        .collect()
}

fn checked_name(value: &str) -> Result<String, ApplicationSourceError> {
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
        return Err(ApplicationSourceError::new(
            ApplicationSourceErrorKind::InvalidName,
        ));
    }
    Ok(value.to_owned())
}

fn checked_path(value: &str) -> Result<String, ApplicationSourceError> {
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
        return Err(ApplicationSourceError::new(
            ApplicationSourceErrorKind::InvalidPath,
        ));
    }
    Ok(value.to_owned())
}

fn required<'a>(
    object: &'a Map<String, Value>,
    key: &str,
) -> Result<&'a Value, ApplicationSourceError> {
    object
        .get(key)
        .ok_or_else(|| ApplicationSourceError::new(ApplicationSourceErrorKind::InvalidShape))
}

fn string<'a>(
    object: &'a Map<String, Value>,
    key: &str,
) -> Result<&'a str, ApplicationSourceError> {
    required(object, key)?
        .as_str()
        .ok_or_else(|| ApplicationSourceError::new(ApplicationSourceErrorKind::InvalidShape))
}

fn positive_u64(object: &Map<String, Value>, key: &str) -> Result<u64, ApplicationSourceError> {
    let value = required(object, key)?
        .as_u64()
        .ok_or_else(|| ApplicationSourceError::new(ApplicationSourceErrorKind::InvalidShape))?;
    if value == 0 {
        return Err(ApplicationSourceError::new(
            ApplicationSourceErrorKind::InvalidShape,
        ));
    }
    Ok(value)
}

fn object<'a>(
    value: &'a Value,
    expected: &[&str],
) -> Result<&'a Map<String, Value>, ApplicationSourceError> {
    let object = value
        .as_object()
        .ok_or_else(|| ApplicationSourceError::new(ApplicationSourceErrorKind::InvalidShape))?;
    if object.len() != expected.len() || !expected.iter().all(|key| object.contains_key(*key)) {
        return Err(ApplicationSourceError::new(
            ApplicationSourceErrorKind::InvalidShape,
        ));
    }
    Ok(object)
}

fn array(
    value: &Value,
    minimum: usize,
    maximum: usize,
) -> Result<&[Value], ApplicationSourceError> {
    let values = value
        .as_array()
        .ok_or_else(|| ApplicationSourceError::new(ApplicationSourceErrorKind::InvalidShape))?;
    if values.len() < minimum || values.len() > maximum {
        return Err(ApplicationSourceError::new(
            ApplicationSourceErrorKind::LimitExceeded,
        ));
    }
    Ok(values)
}

fn ensure_unique<'a>(
    values: impl IntoIterator<Item = &'a str>,
) -> Result<(), ApplicationSourceError> {
    let mut seen = BTreeSet::new();
    if values.into_iter().any(|value| !seen.insert(value)) {
        return Err(ApplicationSourceError::new(
            ApplicationSourceErrorKind::Duplicate,
        ));
    }
    Ok(())
}

fn hex(bytes: &[u8; 32]) -> String {
    use std::fmt::Write as _;

    let mut output = String::with_capacity(64);
    for byte in bytes {
        write!(output, "{byte:02x}").expect("writing to String cannot fail");
    }
    output
}

#[cfg(test)]
mod tests {
    use super::*;

    const SOURCE: &str = r#"{
      "schema": "riffdb.application-source/v1",
      "application": "sample",
      "contract": {
        "source": "riffdb/contract.riff",
        "lineage": "Sample",
        "version": 1
      },
      "generation": {
        "rust": "generated/rust/client.rs",
        "typescript": "generated/typescript/client.ts",
        "mcp": "generated/mcp/tools.json"
      },
      "query_modules": [{
        "name": "sample",
        "version": 1,
        "queries": [{"name": "ItemPage", "source": "riffdb/queries/item_page.riffq"}]
      }],
      "roles": [{
        "name": "SampleApplication",
        "environment": "development",
        "tenant_scope": "global",
        "queries": ["ItemPage"],
        "commands": ["CreateItem"]
      }],
      "seed_inputs": ["riffdb/seed/01-CreateItem.jsonl"]
    }"#;

    #[test]
    fn symbolic_source_is_canonical_and_contains_no_compiler_identity() {
        let manifest = ApplicationSourceManifest::parse(SOURCE).expect("source");
        let canonical = std::str::from_utf8(manifest.canonical_bytes()).expect("canonical UTF-8");
        assert!(!canonical.contains("bundle_hash"));
        assert!(!canonical.contains("module_hash"));
        assert_eq!(
            ApplicationSourceManifest::decode_canonical(manifest.canonical_bytes()),
            Ok(manifest)
        );
    }

    #[test]
    fn symbolic_source_rejects_unknown_members_and_paths() {
        assert_eq!(
            ApplicationSourceManifest::parse(&SOURCE.replace(
                "\"application\": \"sample\",",
                "\"application\": \"sample\", \"unknown\": true,"
            ))
            .expect_err("closed"),
            ApplicationSourceError::new(ApplicationSourceErrorKind::InvalidShape)
        );
        assert_eq!(
            ApplicationSourceManifest::parse(
                &SOURCE.replace("riffdb/contract.riff", "../contract.riff")
            )
            .expect_err("path"),
            ApplicationSourceError::new(ApplicationSourceErrorKind::InvalidPath)
        );
    }

    #[test]
    fn v2_requires_and_canonically_covers_python_target() {
        let source = SOURCE
            .replace("application-source/v1", "application-source/v2")
            .replace(
                "\"mcp\": \"generated/mcp/tools.json\"",
                "\"mcp\": \"generated/mcp/tools.json\",\n        \"python\": \"generated/python/client.py\"",
            );
        let manifest = ApplicationSourceManifest::parse(&source).expect("v2 source");
        assert_eq!(manifest.schema(), APPLICATION_SOURCE_SCHEMA_V2);
        assert_eq!(
            manifest.generation().python(),
            Some("generated/python/client.py")
        );
        assert!(
            std::str::from_utf8(manifest.canonical_bytes())
                .expect("UTF-8")
                .contains("\"python\":\"generated/python/client.py\"")
        );

        let missing = source.replace(",\n        \"python\": \"generated/python/client.py\"", "");
        assert_eq!(
            ApplicationSourceManifest::parse(&missing)
                .expect_err("required Python target")
                .kind(),
            ApplicationSourceErrorKind::InvalidShape
        );
        assert_eq!(
            ApplicationSourceManifest::parse(&SOURCE.replace(
                "\"mcp\": \"generated/mcp/tools.json\"",
                "\"mcp\": \"generated/mcp/tools.json\", \"python\": \"generated/python/client.py\""
            ))
            .expect_err("v1 remains closed")
            .kind(),
            ApplicationSourceErrorKind::InvalidShape
        );
    }
}
