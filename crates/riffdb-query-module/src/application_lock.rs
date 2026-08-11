//! Compiler-owned exact application lock.

use std::collections::BTreeSet;
use std::fmt;

use riffdb_contract_ir::{ContractBundle, MigrationBundleV1};
use riffdb_query_ir::{
    REACTIVE_IR_VERSION_V1, REACTIVE_MODULE_FORMAT_VERSION_V1, ReactiveModulePlanV1,
    ReactiveOperationPlanV1,
};
use riffdb_types::{
    ApplicationLockHash, ApplicationManifestHash, ApplicationSourceHash, ContractBundleHash,
    ContractVersion, GeneratedArtifactHash, MigrationBundleHash, MigrationSourceHash,
    hash_application_lock, hash_application_role_definition, hash_generated_artifact,
    hash_migration_source,
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
/// Exact lock schema pinning direct-parent migration artifacts.
pub const APPLICATION_LOCK_SCHEMA_V4: &str = "riffdb.application-lock/v4";
/// Exact lock schema binding reactive modules and V2 role definitions.
pub const APPLICATION_LOCK_SCHEMA_V5: &str = "riffdb.application-lock/v5";
/// Exact lock schema pinning generated Go bindings.
pub const APPLICATION_LOCK_SCHEMA_V6: &str = "riffdb.application-lock/v6";
/// Compiler-owned canonical contract-bundle artifact path.
pub const CONTRACT_BUNDLE_ARTIFACT_PATH: &str = "generated/riffdb.contract.bundle";
/// Maximum accepted canonical application-lock bytes.
pub const MAX_APPLICATION_LOCK_BYTES: usize = 4 * 1_024 * 1_024;
/// Current tenant-unbound role-definition format.
pub const APPLICATION_ROLE_DEFINITION_FORMAT_V1: u32 = 1;
/// Role definition format with exact reactive operation authority.
pub const APPLICATION_ROLE_DEFINITION_FORMAT_V2: u32 = 2;
const MAX_ARTIFACT_BYTES: usize = 16 * 1_024 * 1_024;
const MAX_ARTIFACTS: usize = 128;

/// Exact decoded inputs for one V4 direct-parent migration lock entry.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ApplicationMigrationLockInput {
    source_path: String,
    source_hash: MigrationSourceHash,
    parent_artifact_path: String,
    parent_version: ContractVersion,
    parent_bundle_hash: ContractBundleHash,
    migration_artifact_path: String,
    migration_bundle: MigrationBundleV1,
}

impl ApplicationMigrationLockInput {
    /// Validates the source and retained-parent identities without I/O.
    pub fn new(
        source_path: impl Into<String>,
        source_bytes: &[u8],
        parent_artifact_path: impl Into<String>,
        parent: &ContractBundle,
        migration_artifact_path: impl Into<String>,
        migration_bundle: &MigrationBundleV1,
    ) -> Result<Self, ApplicationLockError> {
        let source_path = source_path.into();
        let parent_artifact_path = parent_artifact_path.into();
        let migration_artifact_path = migration_artifact_path.into();
        if !valid_path(&source_path)
            || !valid_path(&parent_artifact_path)
            || !valid_path(&migration_artifact_path)
            || source_bytes.len() > riffdb_contract_ir::MAX_MIGRATION_SOURCE_BYTES_V1
        {
            return Err(ApplicationLockError::new(
                ApplicationLockErrorKind::InvalidPath,
            ));
        }
        let source_hash = hash_migration_source(source_bytes);
        if source_hash != migration_bundle.source_hash()
            || migration_bundle.parent_version() != parent.contract_version()
            || migration_bundle.parent_bundle_hash() != parent.bundle_hash()
            || migration_bundle.lineage() != parent.lineage()
        {
            return Err(ApplicationLockError::new(
                ApplicationLockErrorKind::IdentityMismatch,
            ));
        }
        Ok(Self {
            source_path,
            source_hash,
            parent_artifact_path,
            parent_version: parent.contract_version(),
            parent_bundle_hash: parent.bundle_hash(),
            migration_artifact_path,
            migration_bundle: migration_bundle.clone(),
        })
    }
}

/// One canonical V4 direct-parent migration entry.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LockedApplicationMigration {
    source_path: String,
    source_hash: MigrationSourceHash,
    parent_artifact_path: String,
    parent_version: ContractVersion,
    parent_bundle_hash: ContractBundleHash,
    migration_artifact_path: String,
    migration_bundle_hash: MigrationBundleHash,
}

impl LockedApplicationMigration {
    /// Migration source path.
    #[must_use]
    pub fn source_path(&self) -> &str {
        &self.source_path
    }
    /// Exact source hash.
    #[must_use]
    pub const fn source_hash(&self) -> MigrationSourceHash {
        self.source_hash
    }
    /// Retained parent-bundle path.
    #[must_use]
    pub fn parent_artifact_path(&self) -> &str {
        &self.parent_artifact_path
    }
    /// Supported parent version.
    #[must_use]
    pub const fn parent_version(&self) -> ContractVersion {
        self.parent_version
    }
    /// Supported parent bundle hash.
    #[must_use]
    pub const fn parent_bundle_hash(&self) -> ContractBundleHash {
        self.parent_bundle_hash
    }
    /// Canonical migration bundle path.
    #[must_use]
    pub fn migration_artifact_path(&self) -> &str {
        &self.migration_artifact_path
    }
    /// Exact migration bundle hash.
    #[must_use]
    pub const fn migration_bundle_hash(&self) -> MigrationBundleHash {
        self.migration_bundle_hash
    }
}

/// Closed compiler-generated artifact kind.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum GeneratedApplicationArtifactKind {
    /// Compatible exact V1 application manifest.
    Manifest,
    /// Rust application bindings.
    Rust,
    /// TypeScript application bindings.
    TypeScript,
    /// Go application bindings.
    Go,
    /// Python application bindings.
    Python,
    /// MCP operation registry.
    Mcp,
    /// Canonical compiler-owned contract bundle.
    ContractBundle,
    /// Canonical reactive module.
    ReactiveModule,
}

impl GeneratedApplicationArtifactKind {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Manifest => "manifest",
            Self::Rust => "rust",
            Self::TypeScript => "typescript",
            Self::Go => "go",
            Self::Python => "python",
            Self::Mcp => "mcp",
            Self::ContractBundle => "contract_bundle",
            Self::ReactiveModule => "reactive_module",
        }
    }

    fn parse(value: &str) -> Option<Self> {
        Some(match value {
            "manifest" => Self::Manifest,
            "rust" => Self::Rust,
            "typescript" => Self::TypeScript,
            "go" => Self::Go,
            "python" => Self::Python,
            "mcp" => Self::Mcp,
            "contract_bundle" => Self::ContractBundle,
            "reactive_module" => Self::ReactiveModule,
            _ => return None,
        })
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
    artifacts: Vec<GeneratedApplicationArtifact>,
    contract_bundle_artifact: Option<GeneratedApplicationArtifact>,
    migrations: Vec<LockedApplicationMigration>,
}

struct ApplicationLockCompileInputs<'a> {
    reactive_modules: &'a [ReactiveModulePlanV1],
    artifacts: &'a [GeneratedApplicationArtifact],
    pin_contract_bundle: bool,
    migrations: Option<&'a [ApplicationMigrationLockInput]>,
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
        Self::compile_inner(
            source,
            manifest,
            contract,
            modules,
            ApplicationLockCompileInputs {
                reactive_modules: &[],
                artifacts,
                pin_contract_bundle: false,
                migrations: None,
            },
        )
    }

    /// Compiles lock V3 with the exact canonical contract bundle as an artifact.
    pub fn compile_v3(
        source: &ApplicationSourceManifest,
        manifest: &ApplicationManifest,
        contract: &ContractBundle,
        modules: &[QueryModule],
        artifacts: &[GeneratedApplicationArtifact],
    ) -> Result<Self, ApplicationLockError> {
        Self::compile_inner(
            source,
            manifest,
            contract,
            modules,
            ApplicationLockCompileInputs {
                reactive_modules: &[],
                artifacts,
                pin_contract_bundle: true,
                migrations: None,
            },
        )
    }

    /// Compiles lock V4 with every exact direct-parent migration artifact.
    pub fn compile_v4(
        source: &ApplicationSourceManifest,
        manifest: &ApplicationManifest,
        contract: &ContractBundle,
        modules: &[QueryModule],
        artifacts: &[GeneratedApplicationArtifact],
        migrations: &[ApplicationMigrationLockInput],
    ) -> Result<Self, ApplicationLockError> {
        Self::compile_inner(
            source,
            manifest,
            contract,
            modules,
            ApplicationLockCompileInputs {
                reactive_modules: &[],
                artifacts,
                pin_contract_bundle: true,
                migrations: Some(migrations),
            },
        )
    }

    /// Compiles lock V5 with exact reactive modules and direct-parent migrations.
    pub fn compile_v5(
        source: &ApplicationSourceManifest,
        manifest: &ApplicationManifest,
        contract: &ContractBundle,
        modules: &[QueryModule],
        reactive_modules: &[ReactiveModulePlanV1],
        artifacts: &[GeneratedApplicationArtifact],
        migrations: &[ApplicationMigrationLockInput],
    ) -> Result<Self, ApplicationLockError> {
        Self::compile_inner(
            source,
            manifest,
            contract,
            modules,
            ApplicationLockCompileInputs {
                reactive_modules,
                artifacts,
                pin_contract_bundle: true,
                migrations: Some(migrations),
            },
        )
    }

    /// Compiles lock V6 with exact Go bindings, reactive modules, and migrations.
    pub fn compile_v6(
        source: &ApplicationSourceManifest,
        manifest: &ApplicationManifest,
        contract: &ContractBundle,
        modules: &[QueryModule],
        reactive_modules: &[ReactiveModulePlanV1],
        artifacts: &[GeneratedApplicationArtifact],
        migrations: &[ApplicationMigrationLockInput],
    ) -> Result<Self, ApplicationLockError> {
        Self::compile_inner(
            source,
            manifest,
            contract,
            modules,
            ApplicationLockCompileInputs {
                reactive_modules,
                artifacts,
                pin_contract_bundle: true,
                migrations: Some(migrations),
            },
        )
    }

    fn compile_inner(
        source: &ApplicationSourceManifest,
        manifest: &ApplicationManifest,
        contract: &ContractBundle,
        modules: &[QueryModule],
        inputs: ApplicationLockCompileInputs<'_>,
    ) -> Result<Self, ApplicationLockError> {
        let ApplicationLockCompileInputs {
            reactive_modules,
            artifacts,
            pin_contract_bundle,
            migrations: migration_inputs,
        } = inputs;
        if matches!(
            source.schema(),
            crate::APPLICATION_SOURCE_SCHEMA_V3
                | crate::APPLICATION_SOURCE_SCHEMA_V4
                | crate::APPLICATION_SOURCE_SCHEMA_V5
        ) && migration_inputs.is_none()
        {
            return Err(ApplicationLockError::new(
                ApplicationLockErrorKind::UnsupportedVersion,
            ));
        }
        let expected = if matches!(
            source.schema(),
            crate::APPLICATION_SOURCE_SCHEMA_V4 | crate::APPLICATION_SOURCE_SCHEMA_V5
        ) {
            source.exact_manifest_v2(contract, modules, reactive_modules)
        } else {
            source.exact_manifest(contract, modules)
        }
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
        if !matches!(
            source.schema(),
            crate::APPLICATION_SOURCE_SCHEMA_V4 | crate::APPLICATION_SOURCE_SCHEMA_V5
        ) && artifacts
            .iter()
            .any(|artifact| artifact.kind == GeneratedApplicationArtifactKind::ReactiveModule)
        {
            return Err(ApplicationLockError::new(
                ApplicationLockErrorKind::InvalidShape,
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
        if pin_contract_bundle {
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
        }
        let schema = if matches!(
            source.schema(),
            crate::APPLICATION_SOURCE_SCHEMA_V4 | crate::APPLICATION_SOURCE_SCHEMA_V5
        ) {
            if migration_inputs.is_none()
                || (source.schema() == crate::APPLICATION_SOURCE_SCHEMA_V4
                    && manifest.schema() != crate::APPLICATION_MANIFEST_SCHEMA_V2)
                || (source.schema() == crate::APPLICATION_SOURCE_SCHEMA_V5
                    && manifest.schema() != crate::APPLICATION_MANIFEST_SCHEMA_V3)
            {
                return Err(ApplicationLockError::new(
                    ApplicationLockErrorKind::InvalidShape,
                ));
            }
            require_python_artifact(source, &artifacts)?;
            if source.schema() == crate::APPLICATION_SOURCE_SCHEMA_V5 {
                require_go_artifact(source, &artifacts)?;
            }
            if artifacts
                .iter()
                .filter(|artifact| {
                    artifact.kind == GeneratedApplicationArtifactKind::ReactiveModule
                })
                .count()
                != reactive_modules.len()
            {
                return Err(ApplicationLockError::new(
                    ApplicationLockErrorKind::IdentityMismatch,
                ));
            }
            for module in reactive_modules {
                let path = format!(
                    "generated/reactive/{}.riffdb.reactive.module",
                    module.name()
                );
                let expected = GeneratedApplicationArtifact::new(
                    GeneratedApplicationArtifactKind::ReactiveModule,
                    path,
                    module.canonical_bytes(),
                )?;
                if !artifacts.contains(&expected) {
                    return Err(ApplicationLockError::new(
                        ApplicationLockErrorKind::IdentityMismatch,
                    ));
                }
            }
            if source.schema() == crate::APPLICATION_SOURCE_SCHEMA_V5 {
                APPLICATION_LOCK_SCHEMA_V6
            } else {
                APPLICATION_LOCK_SCHEMA_V5
            }
        } else if migration_inputs.is_some() {
            if source.schema() != crate::APPLICATION_SOURCE_SCHEMA_V3 {
                return Err(ApplicationLockError::new(
                    ApplicationLockErrorKind::InvalidShape,
                ));
            }
            require_python_artifact(source, &artifacts)?;
            APPLICATION_LOCK_SCHEMA_V4
        } else if pin_contract_bundle {
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
            if artifacts.iter().any(|artifact| {
                matches!(
                    artifact.kind,
                    GeneratedApplicationArtifactKind::Python
                        | GeneratedApplicationArtifactKind::Go
                        | GeneratedApplicationArtifactKind::ReactiveModule
                )
            }) {
                return Err(ApplicationLockError::new(
                    ApplicationLockErrorKind::InvalidShape,
                ));
            }
            APPLICATION_LOCK_SCHEMA_V1
        };

        let migrations = validate_migration_inputs(
            source,
            contract,
            &artifacts,
            migration_inputs.unwrap_or_default(),
            matches!(
                schema,
                APPLICATION_LOCK_SCHEMA_V4
                    | APPLICATION_LOCK_SCHEMA_V5
                    | APPLICATION_LOCK_SCHEMA_V6
            ),
        )?;
        let mut sorted_modules = modules.iter().collect::<Vec<_>>();
        sorted_modules.sort_by(|left, right| left.name().cmp(right.name()));
        let module_values = sorted_modules
            .iter()
            .map(|module| module_value(module))
            .collect::<Vec<_>>();
        let role_values = manifest
            .roles()
            .iter()
            .map(|role| role_value(role, modules, reactive_modules, contract))
            .collect::<Result<Vec<_>, _>>()?;
        let query_module_format = modules
            .iter()
            .map(QueryModule::format_version)
            .max()
            .unwrap_or(QUERY_MODULE_FORMAT_VERSION_V1);
        let mut value = json!({
            "artifacts": artifacts.iter().map(|artifact| json!({
                "content_hash": hex(artifact.content_hash.as_bytes()),
                "kind": artifact.kind.as_str(),
                "path": artifact.path,
            })).collect::<Vec<_>>(),
            "compiler_formats": {
                "application_role_definition": if matches!(schema, APPLICATION_LOCK_SCHEMA_V5 | APPLICATION_LOCK_SCHEMA_V6) {
                    APPLICATION_ROLE_DEFINITION_FORMAT_V2
                } else { APPLICATION_ROLE_DEFINITION_FORMAT_V1 },
                "contract_bundle": contract.format_version(),
                "contract_grammar": contract.grammar_version(),
                "contract_ir": contract.ir_version(),
                "query_module": query_module_format,
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
        if matches!(
            schema,
            APPLICATION_LOCK_SCHEMA_V5 | APPLICATION_LOCK_SCHEMA_V6
        ) {
            let object = value.as_object_mut().expect("application lock object");
            object.insert(
                "reactive_modules".to_owned(),
                json!(
                    reactive_modules
                        .iter()
                        .map(reactive_module_value)
                        .collect::<Vec<_>>()
                ),
            );
            let formats = object
                .get_mut("compiler_formats")
                .and_then(Value::as_object_mut)
                .expect("compiler formats object");
            formats.insert(
                "reactive_grammar".to_owned(),
                json!(riffdb_query_syntax::REACTIVE_GRAMMAR_VERSION_V1),
            );
            formats.insert("reactive_ir".to_owned(), json!(REACTIVE_IR_VERSION_V1));
            formats.insert(
                "reactive_module".to_owned(),
                json!(REACTIVE_MODULE_FORMAT_VERSION_V1),
            );
        }
        if matches!(
            schema,
            APPLICATION_LOCK_SCHEMA_V4 | APPLICATION_LOCK_SCHEMA_V5 | APPLICATION_LOCK_SCHEMA_V6
        ) {
            let object = value
                .as_object_mut()
                .expect("compiler-created application lock is an object");
            object.insert(
                "migrations".to_owned(),
                json!(migrations.iter().map(migration_value).collect::<Vec<_>>()),
            );
            let formats = object
                .get_mut("compiler_formats")
                .and_then(Value::as_object_mut)
                .expect("compiler-created formats are an object");
            formats.insert(
                "migration_bundle".to_owned(),
                json!(riffdb_contract_ir::MIGRATION_BUNDLE_FORMAT_VERSION_V1),
            );
            formats.insert(
                "migration_grammar".to_owned(),
                json!(riffdb_contract_ir::MIGRATION_GRAMMAR_VERSION_V1),
            );
            formats.insert(
                "migration_ir".to_owned(),
                json!(riffdb_contract_ir::MIGRATION_IR_VERSION_V1),
            );
        }
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
            artifacts,
            contract_bundle_artifact,
            migrations,
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
        let artifacts = required(object, "artifacts")?
            .as_array()
            .ok_or_else(|| ApplicationLockError::new(ApplicationLockErrorKind::InvalidShape))?
            .iter()
            .map(decode_generated_artifact)
            .collect::<Result<Vec<_>, _>>()?;
        let contract_bundle_artifact = artifacts
            .iter()
            .find(|artifact| artifact.kind == GeneratedApplicationArtifactKind::ContractBundle)
            .cloned();
        if matches!(
            schema,
            APPLICATION_LOCK_SCHEMA_V3
                | APPLICATION_LOCK_SCHEMA_V4
                | APPLICATION_LOCK_SCHEMA_V5
                | APPLICATION_LOCK_SCHEMA_V6
        ) && contract_bundle_artifact.is_none()
        {
            return Err(ApplicationLockError::new(
                ApplicationLockErrorKind::InvalidShape,
            ));
        }
        let migrations = if matches!(
            schema,
            APPLICATION_LOCK_SCHEMA_V4 | APPLICATION_LOCK_SCHEMA_V5 | APPLICATION_LOCK_SCHEMA_V6
        ) {
            required(object, "migrations")?
                .as_array()
                .ok_or_else(|| ApplicationLockError::new(ApplicationLockErrorKind::InvalidShape))?
                .iter()
                .map(decode_locked_migration)
                .collect::<Result<Vec<_>, _>>()?
        } else {
            Vec::new()
        };
        Ok(Self {
            schema,
            source_hash,
            manifest_hash,
            identity: hash_application_lock(&canonical),
            canonical_bytes: canonical,
            artifacts,
            contract_bundle_artifact,
            migrations,
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

    /// Exact compiler-generated artifact inventory retained by this lock.
    #[must_use]
    pub fn artifacts(&self) -> &[GeneratedApplicationArtifact] {
        &self.artifacts
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

    /// Direct-parent migrations in canonical parent-artifact order.
    #[must_use]
    pub fn migrations(&self) -> &[LockedApplicationMigration] {
        &self.migrations
    }
}

fn validate_migration_inputs(
    source: &ApplicationSourceManifest,
    candidate: &ContractBundle,
    artifacts: &[GeneratedApplicationArtifact],
    inputs: &[ApplicationMigrationLockInput],
    required: bool,
) -> Result<Vec<LockedApplicationMigration>, ApplicationLockError> {
    if !required {
        if !inputs.is_empty() || !source.migrations().is_empty() {
            return Err(ApplicationLockError::new(
                ApplicationLockErrorKind::InvalidShape,
            ));
        }
        return Ok(Vec::new());
    }
    if inputs.is_empty() || inputs.len() != source.migrations().len() {
        if inputs.is_empty() && source.migrations().is_empty() {
            return Ok(Vec::new());
        }
        return Err(ApplicationLockError::new(
            ApplicationLockErrorKind::IdentityMismatch,
        ));
    }
    let mut inputs = inputs.iter().collect::<Vec<_>>();
    inputs.sort_by(|left, right| {
        left.parent_artifact_path
            .cmp(&right.parent_artifact_path)
            .then_with(|| left.source_path.cmp(&right.source_path))
    });
    let mut paths = artifacts
        .iter()
        .map(|artifact| artifact.path.as_str())
        .collect::<BTreeSet<_>>();
    let mut versions = BTreeSet::new();
    let mut locked = Vec::with_capacity(inputs.len());
    for (declared, input) in source.migrations().iter().zip(inputs) {
        if declared.source() != input.source_path
            || declared.parent_bundle() != input.parent_artifact_path
            || input.migration_bundle.candidate_version() != candidate.contract_version()
            || input.migration_bundle.candidate_bundle_hash() != candidate.bundle_hash()
            || input.migration_bundle.lineage() != candidate.lineage()
            || input.parent_version >= candidate.contract_version()
        {
            return Err(ApplicationLockError::new(
                ApplicationLockErrorKind::IdentityMismatch,
            ));
        }
        if !versions.insert(input.parent_version)
            || !paths.insert(&input.source_path)
            || !paths.insert(&input.parent_artifact_path)
            || !paths.insert(&input.migration_artifact_path)
        {
            return Err(ApplicationLockError::new(
                ApplicationLockErrorKind::Duplicate,
            ));
        }
        locked.push(LockedApplicationMigration {
            source_path: input.source_path.clone(),
            source_hash: input.source_hash,
            parent_artifact_path: input.parent_artifact_path.clone(),
            parent_version: input.parent_version,
            parent_bundle_hash: input.parent_bundle_hash,
            migration_artifact_path: input.migration_artifact_path.clone(),
            migration_bundle_hash: input.migration_bundle.bundle_hash(),
        });
    }
    Ok(locked)
}

fn migration_value(migration: &LockedApplicationMigration) -> Value {
    json!({
        "migration_artifact": {
            "bundle_hash": hex(migration.migration_bundle_hash.as_bytes()),
            "path": migration.migration_artifact_path,
        },
        "parent_artifact": {
            "bundle_hash": hex(migration.parent_bundle_hash.as_bytes()),
            "path": migration.parent_artifact_path,
            "version": migration.parent_version.get(),
        },
        "source_artifact": {
            "path": migration.source_path,
            "source_hash": hex(migration.source_hash.as_bytes()),
        },
    })
}

fn decode_generated_artifact(
    value: &Value,
) -> Result<GeneratedApplicationArtifact, ApplicationLockError> {
    let artifact = value
        .as_object()
        .ok_or_else(|| ApplicationLockError::new(ApplicationLockErrorKind::InvalidShape))?;
    let kind = required(artifact, "kind")?
        .as_str()
        .and_then(GeneratedApplicationArtifactKind::parse)
        .ok_or_else(|| ApplicationLockError::new(ApplicationLockErrorKind::InvalidShape))?;
    Ok(GeneratedApplicationArtifact {
        kind,
        path: required(artifact, "path")?
            .as_str()
            .ok_or_else(|| ApplicationLockError::new(ApplicationLockErrorKind::InvalidShape))?
            .to_owned(),
        content_hash: GeneratedArtifactHash::from_bytes(parse_hash(required(
            artifact,
            "content_hash",
        )?)?),
    })
}

fn decode_locked_migration(
    value: &Value,
) -> Result<LockedApplicationMigration, ApplicationLockError> {
    let entry = exact_object(
        value,
        &["migration_artifact", "parent_artifact", "source_artifact"],
    )?;
    let migration = exact_object(
        required(entry, "migration_artifact")?,
        &["bundle_hash", "path"],
    )?;
    let parent = exact_object(
        required(entry, "parent_artifact")?,
        &["bundle_hash", "path", "version"],
    )?;
    let source = exact_object(
        required(entry, "source_artifact")?,
        &["path", "source_hash"],
    )?;
    let parent_version = required(parent, "version")?
        .as_u64()
        .and_then(ContractVersion::new)
        .ok_or_else(|| ApplicationLockError::new(ApplicationLockErrorKind::InvalidShape))?;
    Ok(LockedApplicationMigration {
        source_path: parse_path(required(source, "path")?)?,
        source_hash: MigrationSourceHash::from_bytes(parse_hash(required(source, "source_hash")?)?),
        parent_artifact_path: parse_path(required(parent, "path")?)?,
        parent_version,
        parent_bundle_hash: ContractBundleHash::from_bytes(parse_hash(required(
            parent,
            "bundle_hash",
        )?)?),
        migration_artifact_path: parse_path(required(migration, "path")?)?,
        migration_bundle_hash: MigrationBundleHash::from_bytes(parse_hash(required(
            migration,
            "bundle_hash",
        )?)?),
    })
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

fn require_go_artifact(
    source: &ApplicationSourceManifest,
    artifacts: &[GeneratedApplicationArtifact],
) -> Result<(), ApplicationLockError> {
    let go_path = source
        .generation()
        .go()
        .ok_or_else(|| ApplicationLockError::new(ApplicationLockErrorKind::IdentityMismatch))?;
    if artifacts
        .iter()
        .filter(|artifact| artifact.kind == GeneratedApplicationArtifactKind::Go)
        .count()
        != 1
        || !artifacts.iter().any(|artifact| {
            artifact.kind == GeneratedApplicationArtifactKind::Go && artifact.path == go_path
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
            "plan_hash": hex(query.plan().identity().as_bytes()),
            "source_hash": hex(query.source_hash().as_bytes()),
        })).collect::<Vec<_>>(),
        "version": module.version().get(),
    })
}

fn role_value(
    role: &crate::ManifestRole,
    modules: &[QueryModule],
    reactive_modules: &[ReactiveModulePlanV1],
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
            "plan_hash": hex(query.plan().identity().as_bytes()),
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
    let reactive_operation = |name: &str, expected: u8| -> Result<Value, ApplicationLockError> {
        let (module, operation) = reactive_modules
            .iter()
            .find_map(|module| module.operation(name).map(|operation| (module, operation)))
            .ok_or_else(|| ApplicationLockError::new(ApplicationLockErrorKind::UnknownOperation))?;
        let kind = match operation.plan() {
            ReactiveOperationPlanV1::Stream { .. } => 1,
            ReactiveOperationPlanV1::Watch { .. } => 2,
            ReactiveOperationPlanV1::Subscription { .. } => 3,
        };
        if kind != expected {
            return Err(ApplicationLockError::new(
                ApplicationLockErrorKind::UnknownOperation,
            ));
        }
        Ok(
            json!({"module_hash": hex(module.identity().as_bytes()), "name": name,
            "operation_hash": hex(operation.identity().as_bytes())}),
        )
    };
    let event_streams = role
        .event_streams()
        .iter()
        .map(|name| reactive_operation(name, 1))
        .collect::<Result<Vec<_>, _>>()?;
    let watch_queries = role
        .watch_queries()
        .iter()
        .map(|name| reactive_operation(name, 2))
        .collect::<Result<Vec<_>, _>>()?;
    let agent_subscriptions = role
        .agent_subscriptions()
        .iter()
        .map(|name| reactive_operation(name, 3))
        .collect::<Result<Vec<_>, _>>()?;
    let definition = if reactive_modules.is_empty() {
        json!({
            "commands": commands,
            "environment": role.environment(),
            "name": role.name(),
            "queries": queries,
            "tenant_scope": scope,
        })
    } else {
        json!({
            "agent_subscriptions": agent_subscriptions,
            "commands": commands, "environment": role.environment(), "event_streams": event_streams,
            "name": role.name(), "queries": queries, "tenant_scope": scope,
            "watch_queries": watch_queries,
        })
    };
    let definition_bytes = serde_json::to_vec(&definition)
        .map_err(|_| ApplicationLockError::new(ApplicationLockErrorKind::InvalidShape))?;
    Ok(json!({
        "definition": definition,
        "definition_hash": hex(hash_application_role_definition(&definition_bytes).as_bytes()),
    }))
}

fn reactive_module_value(module: &ReactiveModulePlanV1) -> Value {
    json!({
        "contract_hash": hex(module.contract_hash().as_bytes()),
        "module_hash": hex(module.identity().as_bytes()),
        "name": module.name(),
        "operations": module.operations().iter().map(|operation| json!({
            "name": operation.name().as_str(),
            "operation_hash": hex(operation.identity().as_bytes()),
        })).collect::<Vec<_>>(),
        "source_hash": hex(module.source_hash().as_bytes()),
        "version": module.version(),
    })
}

fn validate_lock_shape(value: &Value) -> Result<&'static str, ApplicationLockError> {
    let schema_value = value
        .as_object()
        .and_then(|root| root.get("schema"))
        .and_then(Value::as_str)
        .ok_or_else(|| ApplicationLockError::new(ApplicationLockErrorKind::InvalidShape))?;
    let root_keys = if matches!(
        schema_value,
        APPLICATION_LOCK_SCHEMA_V5 | APPLICATION_LOCK_SCHEMA_V6
    ) {
        &[
            "artifacts",
            "compiler_formats",
            "contract",
            "exact_manifest_hash",
            "migrations",
            "modules",
            "reactive_modules",
            "roles",
            "schema",
            "source_hash",
        ][..]
    } else if schema_value == APPLICATION_LOCK_SCHEMA_V4 {
        &[
            "artifacts",
            "compiler_formats",
            "contract",
            "exact_manifest_hash",
            "migrations",
            "modules",
            "roles",
            "schema",
            "source_hash",
        ][..]
    } else {
        &[
            "artifacts",
            "compiler_formats",
            "contract",
            "exact_manifest_hash",
            "modules",
            "roles",
            "schema",
            "source_hash",
        ][..]
    };
    let root = exact_object(value, root_keys)?;
    let schema = match required(root, "schema")?.as_str() {
        Some(APPLICATION_LOCK_SCHEMA_V1) => APPLICATION_LOCK_SCHEMA_V1,
        Some(APPLICATION_LOCK_SCHEMA_V2) => APPLICATION_LOCK_SCHEMA_V2,
        Some(APPLICATION_LOCK_SCHEMA_V3) => APPLICATION_LOCK_SCHEMA_V3,
        Some(APPLICATION_LOCK_SCHEMA_V4) => APPLICATION_LOCK_SCHEMA_V4,
        Some(APPLICATION_LOCK_SCHEMA_V5) => APPLICATION_LOCK_SCHEMA_V5,
        Some(APPLICATION_LOCK_SCHEMA_V6) => APPLICATION_LOCK_SCHEMA_V6,
        _ => {
            return Err(ApplicationLockError::new(
                ApplicationLockErrorKind::UnsupportedVersion,
            ));
        }
    };
    parse_hash(required(root, "source_hash")?)?;
    parse_hash(required(root, "exact_manifest_hash")?)?;
    let format_keys = if matches!(
        schema,
        APPLICATION_LOCK_SCHEMA_V5 | APPLICATION_LOCK_SCHEMA_V6
    ) {
        &[
            "application_role_definition",
            "contract_bundle",
            "contract_grammar",
            "contract_ir",
            "migration_bundle",
            "migration_grammar",
            "migration_ir",
            "query_module",
            "reactive_grammar",
            "reactive_ir",
            "reactive_module",
        ][..]
    } else if schema == APPLICATION_LOCK_SCHEMA_V4 {
        &[
            "application_role_definition",
            "contract_bundle",
            "contract_grammar",
            "contract_ir",
            "migration_bundle",
            "migration_grammar",
            "migration_ir",
            "query_module",
        ][..]
    } else {
        &[
            "application_role_definition",
            "contract_bundle",
            "contract_grammar",
            "contract_ir",
            "query_module",
        ][..]
    };
    let formats = exact_object(required(root, "compiler_formats")?, format_keys)?;
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
    if matches!(
        schema,
        APPLICATION_LOCK_SCHEMA_V5 | APPLICATION_LOCK_SCHEMA_V6
    ) {
        validate_sorted_array(
            required(root, "reactive_modules")?,
            "name",
            validate_reactive_module,
        )?;
    }
    validate_sorted_roles(required(root, "roles")?)?;
    let artifacts = required(root, "artifacts")?;
    validate_sorted_array(artifacts, "path", |value| validate_artifact(value, schema))?;
    if matches!(
        schema,
        APPLICATION_LOCK_SCHEMA_V2
            | APPLICATION_LOCK_SCHEMA_V4
            | APPLICATION_LOCK_SCHEMA_V5
            | APPLICATION_LOCK_SCHEMA_V6
    ) {
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
    if schema == APPLICATION_LOCK_SCHEMA_V6 {
        let go_count = artifacts
            .as_array()
            .ok_or_else(|| ApplicationLockError::new(ApplicationLockErrorKind::InvalidShape))?
            .iter()
            .filter(|value| value.get("kind").and_then(Value::as_str) == Some("go"))
            .count();
        if go_count != 1 {
            return Err(ApplicationLockError::new(
                ApplicationLockErrorKind::InvalidShape,
            ));
        }
    }
    if matches!(
        schema,
        APPLICATION_LOCK_SCHEMA_V3
            | APPLICATION_LOCK_SCHEMA_V4
            | APPLICATION_LOCK_SCHEMA_V5
            | APPLICATION_LOCK_SCHEMA_V6
    ) {
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
    if matches!(
        schema,
        APPLICATION_LOCK_SCHEMA_V4 | APPLICATION_LOCK_SCHEMA_V5 | APPLICATION_LOCK_SCHEMA_V6
    ) {
        let migrations = required(root, "migrations")?;
        let empty_v5 = matches!(
            schema,
            APPLICATION_LOCK_SCHEMA_V5 | APPLICATION_LOCK_SCHEMA_V6
        ) && migrations.as_array().is_some_and(Vec::is_empty);
        if !empty_v5 {
            validate_migration_array(
                migrations,
                required(contract, "version")?.as_u64().ok_or_else(|| {
                    ApplicationLockError::new(ApplicationLockErrorKind::InvalidShape)
                })?,
            )?;
        }
    }
    Ok(schema)
}

fn validate_reactive_module(value: &Value) -> Result<(), ApplicationLockError> {
    let module = exact_object(
        value,
        &[
            "contract_hash",
            "module_hash",
            "name",
            "operations",
            "source_hash",
            "version",
        ],
    )?;
    for key in ["contract_hash", "module_hash", "source_hash"] {
        parse_hash(required(module, key)?)?;
    }
    if required(module, "name")?.as_str().is_none()
        || required(module, "version")?.as_u64().is_none()
    {
        return Err(ApplicationLockError::new(
            ApplicationLockErrorKind::InvalidShape,
        ));
    }
    validate_sorted_array(required(module, "operations")?, "name", |value| {
        let operation = exact_object(value, &["name", "operation_hash"])?;
        parse_hash(required(operation, "operation_hash")?)?;
        if required(operation, "name")?.as_str().is_none() {
            return Err(ApplicationLockError::new(
                ApplicationLockErrorKind::InvalidShape,
            ));
        }
        Ok(())
    })
}

fn validate_migration_array(
    value: &Value,
    candidate_version: u64,
) -> Result<(), ApplicationLockError> {
    let entries = value
        .as_array()
        .ok_or_else(|| ApplicationLockError::new(ApplicationLockErrorKind::InvalidShape))?;
    if entries.is_empty() || entries.len() > crate::MAX_APPLICATION_MIGRATIONS {
        return Err(ApplicationLockError::new(
            ApplicationLockErrorKind::LimitExceeded,
        ));
    }
    let mut previous_path = None::<String>;
    let mut versions = BTreeSet::new();
    let mut paths = BTreeSet::<String>::new();
    for value in entries {
        let migration = decode_locked_migration(value)?;
        if migration.parent_version.get() >= candidate_version
            || !versions.insert(migration.parent_version)
            || !paths.insert(migration.source_path.clone())
            || !paths.insert(migration.parent_artifact_path.clone())
            || !paths.insert(migration.migration_artifact_path.clone())
            || previous_path
                .as_deref()
                .is_some_and(|previous| previous >= migration.parent_artifact_path.as_str())
        {
            return Err(ApplicationLockError::new(
                ApplicationLockErrorKind::NonCanonical,
            ));
        }
        previous_path = Some(migration.parent_artifact_path);
    }
    Ok(())
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
    let definition_value = required(role, "definition")?;
    let is_v2 = definition_value.get("agent_subscriptions").is_some();
    let definition = if is_v2 {
        exact_object(
            definition_value,
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
        exact_object(
            definition_value,
            &["commands", "environment", "name", "queries", "tenant_scope"],
        )?
    };
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
    })?;
    if is_v2 {
        for key in ["event_streams", "watch_queries", "agent_subscriptions"] {
            validate_sorted_array(required(definition, key)?, "name", |value| {
                let operation = exact_object(value, &["module_hash", "name", "operation_hash"])?;
                parse_hash(required(operation, "module_hash")?)?;
                parse_hash(required(operation, "operation_hash")?)?;
                if required(operation, "name")?.as_str().is_none() {
                    return Err(ApplicationLockError::new(
                        ApplicationLockErrorKind::InvalidShape,
                    ));
                }
                Ok(())
            })?;
        }
    }
    Ok(())
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
        && !matches!(
            (schema, kind),
            (
                APPLICATION_LOCK_SCHEMA_V2
                    | APPLICATION_LOCK_SCHEMA_V3
                    | APPLICATION_LOCK_SCHEMA_V4
                    | APPLICATION_LOCK_SCHEMA_V5
                    | APPLICATION_LOCK_SCHEMA_V6,
                "python"
            )
        )
        && !matches!((schema, kind), (APPLICATION_LOCK_SCHEMA_V6, "go"))
        && !matches!(
            (schema, kind),
            (
                APPLICATION_LOCK_SCHEMA_V3
                    | APPLICATION_LOCK_SCHEMA_V4
                    | APPLICATION_LOCK_SCHEMA_V5
                    | APPLICATION_LOCK_SCHEMA_V6,
                "contract_bundle"
            )
        )
        && !(matches!(
            schema,
            APPLICATION_LOCK_SCHEMA_V5 | APPLICATION_LOCK_SCHEMA_V6
        ) && kind == "reactive_module")
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

fn parse_path(value: &Value) -> Result<String, ApplicationLockError> {
    let path = value
        .as_str()
        .ok_or_else(|| ApplicationLockError::new(ApplicationLockErrorKind::InvalidShape))?;
    if !valid_path(path) {
        return Err(ApplicationLockError::new(
            ApplicationLockErrorKind::InvalidPath,
        ));
    }
    Ok(path.to_owned())
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
