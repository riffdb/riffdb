//! Compiler-owned exact application lock.

use std::collections::BTreeSet;
use std::fmt;

use riffdb_contract_ir::ContractBundle;
use riffdb_types::{
    ApplicationLockHash, ApplicationManifestHash, ApplicationSourceHash, GeneratedArtifactHash,
    hash_application_lock, hash_application_role_definition, hash_generated_artifact,
};
use serde_json::{Map, Value, json};

use crate::{
    ApplicationManifest, ApplicationSourceManifest, ManifestTenantScope,
    QUERY_MODULE_FORMAT_VERSION_V1, QueryModule,
};

/// Exact application-lock schema identifier.
pub const APPLICATION_LOCK_SCHEMA_V1: &str = "riffdb.application-lock/v1";
/// Exact lock schema covering a generated Python artifact.
pub const APPLICATION_LOCK_SCHEMA_V2: &str = "riffdb.application-lock/v2";
/// Exact lock schema pinning one canonical contract-bundle artifact.
pub const APPLICATION_LOCK_SCHEMA_V3: &str = "riffdb.application-lock/v3";
/// Compiler-owned canonical contract-bundle artifact path.
pub const CONTRACT_BUNDLE_ARTIFACT_PATH: &str = "generated/riffdb.contract.bundle";
/// Maximum accepted canonical application-lock bytes.
pub const MAX_APPLICATION_LOCK_BYTES: usize = 4 * 1_024 * 1_024;
/// Current tenant-unbound role-definition format.
pub const APPLICATION_ROLE_DEFINITION_FORMAT_V1: u32 = 1;
const MAX_ARTIFACT_BYTES: usize = 16 * 1_024 * 1_024;
const MAX_ARTIFACTS: usize = 128;

/// Closed compiler-generated artifact kind.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum GeneratedApplicationArtifactKind {
    /// Compatible exact V1 application manifest.
    Manifest,
    /// Rust application bindings.
    Rust,
    /// TypeScript application bindings.
    TypeScript,
    /// Python application bindings.
    Python,
    /// MCP operation registry.
    Mcp,
    /// Canonical compiler-owned contract bundle.
    ContractBundle,
}

impl GeneratedApplicationArtifactKind {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Manifest => "manifest",
            Self::Rust => "rust",
            Self::TypeScript => "typescript",
            Self::Python => "python",
            Self::Mcp => "mcp",
            Self::ContractBundle => "contract_bundle",
        }
    }
}

/// One generated output supplied to lock compilation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GeneratedApplicationArtifact {
    kind: GeneratedApplicationArtifactKind,
    path: String,
    content_hash: GeneratedArtifactHash,
}

impl GeneratedApplicationArtifact {
    /// Validates and hashes one bounded generated artifact.
    pub fn new(
        kind: GeneratedApplicationArtifactKind,
        path: impl Into<String>,
        bytes: &[u8],
    ) -> Result<Self, ApplicationLockError> {
        let path = path.into();
        if !valid_path(&path) {
            return Err(ApplicationLockError::new(
                ApplicationLockErrorKind::InvalidPath,
            ));
        }
        if bytes.len() > MAX_ARTIFACT_BYTES {
            return Err(ApplicationLockError::new(
                ApplicationLockErrorKind::LimitExceeded,
            ));
        }
        Ok(Self {
            kind,
            path,
            content_hash: hash_generated_artifact(bytes),
        })
    }

    /// Artifact kind.
    #[must_use]
    pub const fn kind(&self) -> GeneratedApplicationArtifactKind {
        self.kind
    }

    /// Workspace-relative output path.
    #[must_use]
    pub fn path(&self) -> &str {
        &self.path
    }

    /// Domain-separated generated content hash.
    #[must_use]
    pub const fn content_hash(&self) -> GeneratedArtifactHash {
        self.content_hash
    }
}

/// Canonical compiler-owned exact application lock.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ApplicationLock {
    schema: &'static str,
    source_hash: ApplicationSourceHash,
    manifest_hash: ApplicationManifestHash,
    canonical_bytes: Vec<u8>,
    identity: ApplicationLockHash,
    contract_bundle_artifact: Option<GeneratedApplicationArtifact>,
}

impl ApplicationLock {
    /// Compiles the complete exact lock without performing I/O or deployment.
    pub fn compile(
        source: &ApplicationSourceManifest,
        manifest: &ApplicationManifest,
        contract: &ContractBundle,
        modules: &[QueryModule],
        artifacts: &[GeneratedApplicationArtifact],
    ) -> Result<Self, ApplicationLockError> {
        Self::compile_inner(source, manifest, contract, modules, artifacts, false)
    }

    /// Compiles lock V3 with the exact canonical contract bundle as an artifact.
    pub fn compile_v3(
        source: &ApplicationSourceManifest,
        manifest: &ApplicationManifest,
        contract: &ContractBundle,
        modules: &[QueryModule],
        artifacts: &[GeneratedApplicationArtifact],
    ) -> Result<Self, ApplicationLockError> {
        Self::compile_inner(source, manifest, contract, modules, artifacts, true)
    }

    fn compile_inner(
        source: &ApplicationSourceManifest,
        manifest: &ApplicationManifest,
        contract: &ContractBundle,
        modules: &[QueryModule],
        artifacts: &[GeneratedApplicationArtifact],
        pin_contract_bundle: bool,
    ) -> Result<Self, ApplicationLockError> {
        let expected = source
            .exact_manifest(contract, modules)
            .map_err(|_| ApplicationLockError::new(ApplicationLockErrorKind::IdentityMismatch))?;
        if expected.canonical_bytes() != manifest.canonical_bytes() {
            return Err(ApplicationLockError::new(
                ApplicationLockErrorKind::IdentityMismatch,
            ));
        }
        if artifacts.len() > MAX_ARTIFACTS {
            return Err(ApplicationLockError::new(
                ApplicationLockErrorKind::LimitExceeded,
            ));
        }
        let mut artifacts = artifacts.to_vec();
        artifacts.sort_by(|left, right| {
            (left.path.as_str(), left.kind).cmp(&(right.path.as_str(), right.kind))
        });
        if artifacts
            .windows(2)
            .any(|pair| pair[0].path == pair[1].path)
        {
            return Err(ApplicationLockError::new(
                ApplicationLockErrorKind::Duplicate,
            ));
        }
        let contract_bundle_artifact = artifacts
            .iter()
            .find(|artifact| artifact.kind == GeneratedApplicationArtifactKind::ContractBundle)
            .cloned();
        if !pin_contract_bundle && contract_bundle_artifact.is_some() {
            return Err(ApplicationLockError::new(
                ApplicationLockErrorKind::InvalidShape,
            ));
        }
        let schema = if pin_contract_bundle {
            let expected_bundle_artifact = GeneratedApplicationArtifact::new(
                GeneratedApplicationArtifactKind::ContractBundle,
                CONTRACT_BUNDLE_ARTIFACT_PATH,
                contract.canonical_bytes(),
            )?;
            if contract_bundle_artifact.as_ref() != Some(&expected_bundle_artifact)
                || artifacts
                    .iter()
                    .filter(|artifact| {
                        artifact.kind == GeneratedApplicationArtifactKind::ContractBundle
                    })
                    .count()
                    != 1
            {
                return Err(ApplicationLockError::new(
                    ApplicationLockErrorKind::IdentityMismatch,
                ));
            }
            if source.schema() == crate::APPLICATION_SOURCE_SCHEMA_V2 {
                require_python_artifact(source, &artifacts)?;
            } else if artifacts
                .iter()
                .any(|artifact| artifact.kind == GeneratedApplicationArtifactKind::Python)
            {
                return Err(ApplicationLockError::new(
                    ApplicationLockErrorKind::InvalidShape,
                ));
            }
            APPLICATION_LOCK_SCHEMA_V3
        } else if source.schema() == crate::APPLICATION_SOURCE_SCHEMA_V2 {
            let python_path = source.generation().python().ok_or_else(|| {
                ApplicationLockError::new(ApplicationLockErrorKind::IdentityMismatch)
            })?;
            if artifacts
                .iter()
                .filter(|artifact| artifact.kind == GeneratedApplicationArtifactKind::Python)
                .count()
                != 1
                || !artifacts.iter().any(|artifact| {
                    artifact.kind == GeneratedApplicationArtifactKind::Python
                        && artifact.path == python_path
                })
            {
                return Err(ApplicationLockError::new(
                    ApplicationLockErrorKind::IdentityMismatch,
                ));
            }
            APPLICATION_LOCK_SCHEMA_V2
        } else {
            if artifacts
                .iter()
                .any(|artifact| artifact.kind == GeneratedApplicationArtifactKind::Python)
            {
                return Err(ApplicationLockError::new(
                    ApplicationLockErrorKind::InvalidShape,
                ));
            }
            APPLICATION_LOCK_SCHEMA_V1
        };

        let mut sorted_modules = modules.iter().collect::<Vec<_>>();
        sorted_modules.sort_by(|left, right| left.name().cmp(right.name()));
        let module_values = sorted_modules
            .iter()
            .map(|module| module_value(module))
            .collect::<Vec<_>>();
        let role_values = manifest
            .roles()
            .iter()
            .map(|role| role_value(role, modules, contract))
            .collect::<Result<Vec<_>, _>>()?;
        let value = json!({
            "artifacts": artifacts.iter().map(|artifact| json!({
                "content_hash": hex(artifact.content_hash.as_bytes()),
                "kind": artifact.kind.as_str(),
                "path": artifact.path,
            })).collect::<Vec<_>>(),
            "compiler_formats": {
                "application_role_definition": APPLICATION_ROLE_DEFINITION_FORMAT_V1,
                "contract_bundle": contract.format_version(),
                "contract_grammar": contract.grammar_version(),
                "contract_ir": contract.ir_version(),
                "query_module": QUERY_MODULE_FORMAT_VERSION_V1,
            },
            "contract": {
                "bundle_hash": hex(contract.bundle_hash().as_bytes()),
                "compiler": contract.compiler_version(),
                "lineage": contract.lineage().as_str(),
                "plan_root_hash": hex(contract.plan_root_hash().as_bytes()),
                "source_hash": hex(contract.source_hash().as_bytes()),
                "version": contract.contract_version().get(),
            },
            "exact_manifest_hash": hex(manifest.identity().as_bytes()),
            "modules": module_values,
            "roles": role_values,
            "schema": schema,
            "source_hash": hex(source.identity().as_bytes()),
        });
        let mut canonical_bytes = serde_json::to_vec(&value)
            .map_err(|_| ApplicationLockError::new(ApplicationLockErrorKind::InvalidShape))?;
        canonical_bytes.push(b'\n');
        if canonical_bytes.len() > MAX_APPLICATION_LOCK_BYTES {
            return Err(ApplicationLockError::new(
                ApplicationLockErrorKind::LimitExceeded,
            ));
        }
        Ok(Self {
            schema,
            source_hash: source.identity(),
            manifest_hash: manifest.identity(),
            identity: hash_application_lock(&canonical_bytes),
            canonical_bytes,
            contract_bundle_artifact,
        })
    }

    /// Strictly decodes and structurally validates canonical lock bytes.
    pub fn decode_canonical(bytes: &[u8]) -> Result<Self, ApplicationLockError> {
        if bytes.is_empty() || bytes.len() > MAX_APPLICATION_LOCK_BYTES {
            return Err(ApplicationLockError::new(
                ApplicationLockErrorKind::LimitExceeded,
            ));
        }
        let value: Value = serde_json::from_slice(bytes)
            .map_err(|_| ApplicationLockError::new(ApplicationLockErrorKind::InvalidJson))?;
        let schema = validate_lock_shape(&value)?;
        let mut canonical = serde_json::to_vec(&value)
            .map_err(|_| ApplicationLockError::new(ApplicationLockErrorKind::InvalidJson))?;
        canonical.push(b'\n');
        if canonical != bytes {
            return Err(ApplicationLockError::new(
                ApplicationLockErrorKind::NonCanonical,
            ));
        }
        let object = value
            .as_object()
            .ok_or_else(|| ApplicationLockError::new(ApplicationLockErrorKind::InvalidShape))?;
        let source_hash =
            ApplicationSourceHash::from_bytes(parse_hash(required(object, "source_hash")?)?);
        let manifest_hash = ApplicationManifestHash::from_bytes(parse_hash(required(
            object,
            "exact_manifest_hash",
        )?)?);
        let contract_bundle_artifact = if schema == APPLICATION_LOCK_SCHEMA_V3 {
            let artifacts = required(object, "artifacts")?
                .as_array()
                .ok_or_else(|| ApplicationLockError::new(ApplicationLockErrorKind::InvalidShape))?;
            let value = artifacts
                .iter()
                .find(|artifact| {
                    artifact.get("kind").and_then(Value::as_str) == Some("contract_bundle")
                })
                .ok_or_else(|| ApplicationLockError::new(ApplicationLockErrorKind::InvalidShape))?;
            let artifact = value
                .as_object()
                .ok_or_else(|| ApplicationLockError::new(ApplicationLockErrorKind::InvalidShape))?;
            Some(GeneratedApplicationArtifact {
                kind: GeneratedApplicationArtifactKind::ContractBundle,
                path: required(artifact, "path")?
                    .as_str()
                    .ok_or_else(|| {
                        ApplicationLockError::new(ApplicationLockErrorKind::InvalidShape)
                    })?
                    .to_owned(),
                content_hash: GeneratedArtifactHash::from_bytes(parse_hash(required(
                    artifact,
                    "content_hash",
                )?)?),
            })
        } else {
            None
        };
        Ok(Self {
            schema,
            source_hash,
            manifest_hash,
            identity: hash_application_lock(&canonical),
            canonical_bytes: canonical,
            contract_bundle_artifact,
        })
    }

    /// Exact author-source identity.
    #[must_use]
    pub const fn source_hash(&self) -> ApplicationSourceHash {
        self.source_hash
    }

    /// Exact lock schema identifier.
    #[must_use]
    pub const fn schema(&self) -> &'static str {
        self.schema
    }

    /// Exact compatible V1 manifest identity.
    #[must_use]
    pub const fn manifest_hash(&self) -> ApplicationManifestHash {
        self.manifest_hash
    }

    /// Canonical lock bytes.
    #[must_use]
    pub fn canonical_bytes(&self) -> &[u8] {
        &self.canonical_bytes
    }

    /// Domain-separated lock identity.
    #[must_use]
    pub const fn identity(&self) -> ApplicationLockHash {
        self.identity
    }

    /// Exact canonical contract-bundle artifact pinned by lock V3.
    #[must_use]
    pub const fn contract_bundle_artifact(&self) -> Option<&GeneratedApplicationArtifact> {
        self.contract_bundle_artifact.as_ref()
    }
}

fn require_python_artifact(
    source: &ApplicationSourceManifest,
    artifacts: &[GeneratedApplicationArtifact],
) -> Result<(), ApplicationLockError> {
    let python_path = source
        .generation()
        .python()
        .ok_or_else(|| ApplicationLockError::new(ApplicationLockErrorKind::IdentityMismatch))?;
    if artifacts
        .iter()
        .filter(|artifact| artifact.kind == GeneratedApplicationArtifactKind::Python)
        .count()
        != 1
        || !artifacts.iter().any(|artifact| {
            artifact.kind == GeneratedApplicationArtifactKind::Python
                && artifact.path == python_path
        })
    {
        return Err(ApplicationLockError::new(
            ApplicationLockErrorKind::IdentityMismatch,
        ));
    }
    Ok(())
}

/// Closed application-lock failure kind.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ApplicationLockErrorKind {
    /// Invalid JSON.
    InvalidJson,
    /// Unsupported lock schema.
    UnsupportedVersion,
    /// Missing, extra, or wrongly typed data.
    InvalidShape,
    /// Invalid workspace-relative artifact path.
    InvalidPath,
    /// Duplicate declaration or artifact target.
    Duplicate,
    /// A hard lock or artifact bound was exceeded.
    LimitExceeded,
    /// Canonical decoder observed another spelling or order.
    NonCanonical,
    /// Source, exact manifest, contract, module, role, or artifact identity disagrees.
    IdentityMismatch,
    /// A role names an operation absent from the compiled application.
    UnknownOperation,
}

/// Bounded redaction-safe application-lock error.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ApplicationLockError {
    kind: ApplicationLockErrorKind,
}

impl ApplicationLockError {
    const fn new(kind: ApplicationLockErrorKind) -> Self {
        Self { kind }
    }

    /// Closed failure kind.
    #[must_use]
    pub const fn kind(self) -> ApplicationLockErrorKind {
        self.kind
    }
}

impl fmt::Display for ApplicationLockError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self.kind {
            ApplicationLockErrorKind::InvalidJson => "application lock JSON is invalid",
            ApplicationLockErrorKind::UnsupportedVersion => {
                "application lock version is unsupported"
            }
            ApplicationLockErrorKind::InvalidShape => "application lock shape is invalid",
            ApplicationLockErrorKind::InvalidPath => "generated artifact path is invalid",
            ApplicationLockErrorKind::Duplicate => {
                "application lock contains a duplicate declaration"
            }
            ApplicationLockErrorKind::LimitExceeded => "application lock limit exceeded",
            ApplicationLockErrorKind::NonCanonical => "application lock bytes are not canonical",
            ApplicationLockErrorKind::IdentityMismatch => {
                "application source and compiled identities do not match"
            }
            ApplicationLockErrorKind::UnknownOperation => {
                "application role names an unknown compiled operation"
            }
        })
    }
}

impl std::error::Error for ApplicationLockError {}

fn module_value(module: &QueryModule) -> Value {
    json!({
        "contract_hash": hex(module.contract_hash().as_bytes()),
        "module_hash": hex(module.identity().as_bytes()),
        "name": module.name().as_str(),
        "queries": module.queries().iter().map(|query| json!({
            "name": query.name(),
            "plan_hash": hex(query.program().identity().hash().as_bytes()),
            "source_hash": hex(query.source_hash().as_bytes()),
        })).collect::<Vec<_>>(),
        "version": module.version().get(),
    })
}

fn role_value(
    role: &crate::ManifestRole,
    modules: &[QueryModule],
    contract: &ContractBundle,
) -> Result<Value, ApplicationLockError> {
    let mut queries = Vec::with_capacity(role.queries().len());
    for name in role.queries() {
        let (module, query) = modules
            .iter()
            .find_map(|module| module.query(name).map(|query| (module, query)))
            .ok_or_else(|| ApplicationLockError::new(ApplicationLockErrorKind::UnknownOperation))?;
        queries.push(json!({
            "module_hash": hex(module.identity().as_bytes()),
            "name": name,
            "plan_hash": hex(query.program().identity().hash().as_bytes()),
        }));
    }
    let mut commands = Vec::with_capacity(role.commands().len());
    for name in role.commands() {
        let command = contract
            .commands()
            .iter()
            .find(|command| command.name() == name)
            .ok_or_else(|| ApplicationLockError::new(ApplicationLockErrorKind::UnknownOperation))?;
        commands.push(json!({
            "name": name,
            "plan_hash": hex(command.plan_hash().as_bytes()),
        }));
    }
    let scope = match role.tenant_scope() {
        ManifestTenantScope::Global => "global",
        ManifestTenantScope::Tenant => "tenant",
    };
    let definition = json!({
        "commands": commands,
        "environment": role.environment(),
        "name": role.name(),
        "queries": queries,
        "tenant_scope": scope,
    });
    let definition_bytes = serde_json::to_vec(&definition)
        .map_err(|_| ApplicationLockError::new(ApplicationLockErrorKind::InvalidShape))?;
    Ok(json!({
        "definition": definition,
        "definition_hash": hex(hash_application_role_definition(&definition_bytes).as_bytes()),
    }))
}

fn validate_lock_shape(value: &Value) -> Result<&'static str, ApplicationLockError> {
    let root = exact_object(
        value,
        &[
            "artifacts",
            "compiler_formats",
            "contract",
            "exact_manifest_hash",
            "modules",
            "roles",
            "schema",
            "source_hash",
        ],
    )?;
    let schema = match required(root, "schema")?.as_str() {
        Some(APPLICATION_LOCK_SCHEMA_V1) => APPLICATION_LOCK_SCHEMA_V1,
        Some(APPLICATION_LOCK_SCHEMA_V2) => APPLICATION_LOCK_SCHEMA_V2,
        Some(APPLICATION_LOCK_SCHEMA_V3) => APPLICATION_LOCK_SCHEMA_V3,
        _ => {
            return Err(ApplicationLockError::new(
                ApplicationLockErrorKind::UnsupportedVersion,
            ));
        }
    };
    parse_hash(required(root, "source_hash")?)?;
    parse_hash(required(root, "exact_manifest_hash")?)?;
    let formats = exact_object(
        required(root, "compiler_formats")?,
        &[
            "application_role_definition",
            "contract_bundle",
            "contract_grammar",
            "contract_ir",
            "query_module",
        ],
    )?;
    if formats.values().any(|value| value.as_u64().is_none()) {
        return Err(ApplicationLockError::new(
            ApplicationLockErrorKind::InvalidShape,
        ));
    }
    let contract = exact_object(
        required(root, "contract")?,
        &[
            "bundle_hash",
            "compiler",
            "lineage",
            "plan_root_hash",
            "source_hash",
            "version",
        ],
    )?;
    for key in ["bundle_hash", "plan_root_hash", "source_hash"] {
        parse_hash(required(contract, key)?)?;
    }
    if required(contract, "compiler")?.as_str().is_none()
        || required(contract, "lineage")?.as_str().is_none()
        || required(contract, "version")?.as_u64().is_none()
    {
        return Err(ApplicationLockError::new(
            ApplicationLockErrorKind::InvalidShape,
        ));
    }
    validate_sorted_array(required(root, "modules")?, "name", validate_module)?;
    validate_sorted_roles(required(root, "roles")?)?;
    let artifacts = required(root, "artifacts")?;
    validate_sorted_array(artifacts, "path", |value| validate_artifact(value, schema))?;
    if schema == APPLICATION_LOCK_SCHEMA_V2 {
        let python_count = artifacts
            .as_array()
            .ok_or_else(|| ApplicationLockError::new(ApplicationLockErrorKind::InvalidShape))?
            .iter()
            .filter(|value| value.get("kind").and_then(Value::as_str) == Some("python"))
            .count();
        if python_count != 1 {
            return Err(ApplicationLockError::new(
                ApplicationLockErrorKind::InvalidShape,
            ));
        }
    }
    if schema == APPLICATION_LOCK_SCHEMA_V3 {
        let contract_bundles = artifacts
            .as_array()
            .ok_or_else(|| ApplicationLockError::new(ApplicationLockErrorKind::InvalidShape))?
            .iter()
            .filter(|value| value.get("kind").and_then(Value::as_str) == Some("contract_bundle"))
            .collect::<Vec<_>>();
        if contract_bundles.len() != 1
            || contract_bundles[0].get("path").and_then(Value::as_str)
                != Some(CONTRACT_BUNDLE_ARTIFACT_PATH)
        {
            return Err(ApplicationLockError::new(
                ApplicationLockErrorKind::InvalidShape,
            ));
        }
    }
    Ok(schema)
}

fn validate_module(value: &Value) -> Result<(), ApplicationLockError> {
    let module = exact_object(
        value,
        &["contract_hash", "module_hash", "name", "queries", "version"],
    )?;
    parse_hash(required(module, "contract_hash")?)?;
    parse_hash(required(module, "module_hash")?)?;
    if required(module, "name")?.as_str().is_none()
        || required(module, "version")?.as_u64().is_none()
    {
        return Err(ApplicationLockError::new(
            ApplicationLockErrorKind::InvalidShape,
        ));
    }
    validate_sorted_array(required(module, "queries")?, "name", |value| {
        let query = exact_object(value, &["name", "plan_hash", "source_hash"])?;
        parse_hash(required(query, "plan_hash")?)?;
        parse_hash(required(query, "source_hash")?)?;
        if required(query, "name")?.as_str().is_none() {
            return Err(ApplicationLockError::new(
                ApplicationLockErrorKind::InvalidShape,
            ));
        }
        Ok(())
    })
}

fn validate_role(value: &Value) -> Result<(), ApplicationLockError> {
    let role = exact_object(value, &["definition", "definition_hash"])?;
    let stored_hash = parse_hash(required(role, "definition_hash")?)?;
    let definition = exact_object(
        required(role, "definition")?,
        &["commands", "environment", "name", "queries", "tenant_scope"],
    )?;
    let definition_bytes = serde_json::to_vec(required(role, "definition")?)
        .map_err(|_| ApplicationLockError::new(ApplicationLockErrorKind::InvalidShape))?;
    if hash_application_role_definition(&definition_bytes).as_bytes() != &stored_hash {
        return Err(ApplicationLockError::new(
            ApplicationLockErrorKind::IdentityMismatch,
        ));
    }
    for key in ["environment", "name", "tenant_scope"] {
        if required(definition, key)?.as_str().is_none() {
            return Err(ApplicationLockError::new(
                ApplicationLockErrorKind::InvalidShape,
            ));
        }
    }
    validate_sorted_array(required(definition, "queries")?, "name", |value| {
        let query = exact_object(value, &["module_hash", "name", "plan_hash"])?;
        parse_hash(required(query, "module_hash")?)?;
        parse_hash(required(query, "plan_hash")?)?;
        if required(query, "name")?.as_str().is_none() {
            return Err(ApplicationLockError::new(
                ApplicationLockErrorKind::InvalidShape,
            ));
        }
        Ok(())
    })?;
    validate_sorted_array(required(definition, "commands")?, "name", |value| {
        let command = exact_object(value, &["name", "plan_hash"])?;
        parse_hash(required(command, "plan_hash")?)?;
        if required(command, "name")?.as_str().is_none() {
            return Err(ApplicationLockError::new(
                ApplicationLockErrorKind::InvalidShape,
            ));
        }
        Ok(())
    })
}

fn validate_sorted_roles(value: &Value) -> Result<(), ApplicationLockError> {
    let values = value
        .as_array()
        .ok_or_else(|| ApplicationLockError::new(ApplicationLockErrorKind::InvalidShape))?;
    let mut previous = None;
    for value in values {
        validate_role(value)?;
        let role = value
            .as_object()
            .and_then(|object| object.get("definition"))
            .and_then(Value::as_object)
            .and_then(|object| object.get("name"))
            .and_then(Value::as_str)
            .ok_or_else(|| ApplicationLockError::new(ApplicationLockErrorKind::InvalidShape))?;
        if previous.is_some_and(|previous| previous >= role) {
            return Err(ApplicationLockError::new(
                ApplicationLockErrorKind::NonCanonical,
            ));
        }
        previous = Some(role);
    }
    Ok(())
}

fn validate_artifact(value: &Value, schema: &str) -> Result<(), ApplicationLockError> {
    let artifact = exact_object(value, &["content_hash", "kind", "path"])?;
    parse_hash(required(artifact, "content_hash")?)?;
    let kind = required(artifact, "kind")?
        .as_str()
        .ok_or_else(|| ApplicationLockError::new(ApplicationLockErrorKind::InvalidShape))?;
    if !matches!(kind, "manifest" | "rust" | "typescript" | "mcp")
        && !((schema == APPLICATION_LOCK_SCHEMA_V2 || schema == APPLICATION_LOCK_SCHEMA_V3)
            && kind == "python")
        && !(schema == APPLICATION_LOCK_SCHEMA_V3 && kind == "contract_bundle")
    {
        return Err(ApplicationLockError::new(
            ApplicationLockErrorKind::InvalidShape,
        ));
    }
    let path = required(artifact, "path")?
        .as_str()
        .ok_or_else(|| ApplicationLockError::new(ApplicationLockErrorKind::InvalidShape))?;
    if !valid_path(path) {
        return Err(ApplicationLockError::new(
            ApplicationLockErrorKind::InvalidPath,
        ));
    }
    Ok(())
}

fn validate_sorted_array(
    value: &Value,
    sort_key: &str,
    validate: impl Fn(&Value) -> Result<(), ApplicationLockError>,
) -> Result<(), ApplicationLockError> {
    let values = value
        .as_array()
        .ok_or_else(|| ApplicationLockError::new(ApplicationLockErrorKind::InvalidShape))?;
    let mut previous = None;
    let mut seen = BTreeSet::new();
    for value in values {
        validate(value)?;
        let object = value
            .as_object()
            .ok_or_else(|| ApplicationLockError::new(ApplicationLockErrorKind::InvalidShape))?;
        let key = required(object, sort_key)?
            .as_str()
            .ok_or_else(|| ApplicationLockError::new(ApplicationLockErrorKind::InvalidShape))?;
        if previous.is_some_and(|previous| previous >= key) || !seen.insert(key) {
            return Err(ApplicationLockError::new(
                ApplicationLockErrorKind::NonCanonical,
            ));
        }
        previous = Some(key);
    }
    Ok(())
}

fn exact_object<'a>(
    value: &'a Value,
    keys: &[&str],
) -> Result<&'a Map<String, Value>, ApplicationLockError> {
    let object = value
        .as_object()
        .ok_or_else(|| ApplicationLockError::new(ApplicationLockErrorKind::InvalidShape))?;
    if object.len() != keys.len() || !keys.iter().all(|key| object.contains_key(*key)) {
        return Err(ApplicationLockError::new(
            ApplicationLockErrorKind::InvalidShape,
        ));
    }
    Ok(object)
}

fn required<'a>(
    object: &'a Map<String, Value>,
    key: &str,
) -> Result<&'a Value, ApplicationLockError> {
    object
        .get(key)
        .ok_or_else(|| ApplicationLockError::new(ApplicationLockErrorKind::InvalidShape))
}

fn parse_hash(value: &Value) -> Result<[u8; 32], ApplicationLockError> {
    let value = value
        .as_str()
        .ok_or_else(|| ApplicationLockError::new(ApplicationLockErrorKind::InvalidShape))?;
    if value.len() != 64 {
        return Err(ApplicationLockError::new(
            ApplicationLockErrorKind::InvalidShape,
        ));
    }
    let mut bytes = [0_u8; 32];
    for (index, pair) in value.as_bytes().chunks_exact(2).enumerate() {
        let high = hex_nibble(pair[0])?;
        let low = hex_nibble(pair[1])?;
        bytes[index] = (high << 4) | low;
    }
    Ok(bytes)
}

fn hex_nibble(byte: u8) -> Result<u8, ApplicationLockError> {
    match byte {
        b'0'..=b'9' => Ok(byte - b'0'),
        b'a'..=b'f' => Ok(byte - b'a' + 10),
        _ => Err(ApplicationLockError::new(
            ApplicationLockErrorKind::InvalidShape,
        )),
    }
}

fn valid_path(path: &str) -> bool {
    !path.is_empty()
        && path.len() <= 512
        && !path.starts_with('/')
        && !path.starts_with('\\')
        && !path.contains('\\')
        && !path.split('/').any(|part| {
            part.is_empty()
                || part == "."
                || part == ".."
                || part.bytes().any(|byte| byte.is_ascii_control())
        })
}

fn hex(bytes: &[u8; 32]) -> String {
    use std::fmt::Write as _;

    let mut output = String::with_capacity(64);
    for byte in bytes {
        write!(output, "{byte:02x}").expect("writing to String cannot fail");
    }
    output
}
